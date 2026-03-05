import asyncio
import contextlib
import ctypes
import logging
import threading
from io import BytesIO

from google.cloud.storage.asyncio.async_appendable_object_writer import (
    _DEFAULT_FLUSH_INTERVAL_BYTES,
    AsyncAppendableObjectWriter,
)

PyBytes_FromStringAndSize = ctypes.pythonapi.PyBytes_FromStringAndSize
PyBytes_FromStringAndSize.argtypes = (ctypes.c_void_p, ctypes.c_ssize_t)
PyBytes_FromStringAndSize.restype = ctypes.py_object

PyBytes_AsString = ctypes.pythonapi.PyBytes_AsString
PyBytes_AsString.argtypes = (ctypes.py_object,)
PyBytes_AsString.restype = ctypes.c_void_p

logger = logging.getLogger("gcsfs")


_AUTO = "auto"


async def download_range(offset, length, mrd):
    """
    Downloads a byte range from the file asynchronously.
    """
    # If length = 0, mrd returns till end of file, so handle that case here
    if length == 0:
        return b""
    buffer = BytesIO()
    await mrd.download_ranges([(offset, length, buffer)])
    data = buffer.getvalue()
    logger.debug(
        f"Requested {length} bytes from offset {offset}, downloaded {len(data)} bytes"
    )
    return data


async def init_aaow(
    grpc_client, bucket_name, object_name, generation=None, flush_interval_bytes=None
):
    """
    Creates and opens the AsyncAppendableObjectWriter.
    """
    writer_options = {}
    # Only pass flush_interval_bytes if the user explicitly provided a
    # non-default flush interval.
    if flush_interval_bytes and flush_interval_bytes != _DEFAULT_FLUSH_INTERVAL_BYTES:
        writer_options["FLUSH_INTERVAL_BYTES"] = flush_interval_bytes
    writer = AsyncAppendableObjectWriter(
        client=grpc_client,
        bucket_name=bucket_name,
        object_name=object_name,
        generation=generation,
        writer_options=writer_options,
    )
    await writer.open()
    return writer


class DirectMemmoveBuffer:
    """
    A buffer-like object that writes data directly to memory asynchronously.

    This class provides a `write` interface that queues `ctypes.memmove` operations
    to a thread pool executor, limiting the maximum number of concurrent pending
    writes using a semaphore. It is useful for high-performance data transfers
    where memory copies need to be offloaded from the main thread.
    """

    def __init__(self, start_address, executor, max_pending=5):
        """
        Initializes the DirectMemmoveBuffer.

        Args:
            start_address (int): The starting memory address where data will be written.
            executor (concurrent.futures.Executor): The thread pool executor to run the
                memmove operations.
            max_pending (int, optional): The maximum number of pending write operations
                allowed in the queue. Defaults to 5.
        """
        self.start_address = start_address
        self.current_offset = 0
        self.semaphore = threading.Semaphore(max_pending)
        self._error = None
        self._pending_count = 0
        self._lock = threading.Lock()
        self._done_event = threading.Event()
        self._done_event.set()
        self.executor = executor

    def write(self, data):
        """
        Schedules a write operation to memory.

        Calculates the destination address based on the current offset, increments the offset,
        and submits the memory move operation to the executor. Blocks if the number of
        pending operations reaches `max_pending`.

        Args:
            data (bytes): The data to be written to memory.

        Returns:
            concurrent.futures.Future: A future object representing the execution of the
                memory move operation.

        Raises:
            Exception: If any previous asynchronous write operation encountered an error.
        """
        if self._error:
            raise self._error
        size = len(data)
        dest = self.start_address + self.current_offset
        self.current_offset += size
        data_bytes = bytes(data) if not isinstance(data, bytes) else data

        self.semaphore.acquire()
        with self._lock:
            if self._pending_count == 0:
                self._done_event.clear()
            self._pending_count += 1
        return self.executor.submit(self._do_memmove, dest, data_bytes, size)

    def _do_memmove(self, dest, data_bytes, size):
        try:
            ctypes.memmove(dest, data_bytes, size)
        except Exception as e:
            self._error = e
            raise e
        finally:
            self.semaphore.release()
            with self._lock:
                self._pending_count -= 1
                if self._pending_count == 0:
                    self._done_event.set()

    def close(self):
        """
        Waits for all pending write operations to complete and checks for errors.
        Blocks the calling thread until the queue of memory operations is entirely
        processed.

        Raises:
            Exception: If any background write operation failed during execution.
        """
        self._done_event.wait()
        if self._error:
            raise self._error


class MRDPool:
    """Manages a pool of AsyncMultiRangeDownloader objects with on-demand scaling."""

    def __init__(self, gcsfs, bucket_name, object_name, generation, pool_size):
        """
        Initializes the MRDPool.

        Args:
            gcsfs (gcsfs.GCSFileSystem): The GCS filesystem client used for the downloads.
            bucket_name (str): The name of the GCS bucket.
            object_name (str): The target object/blob name in the bucket.
            generation (int or str): The specific generation of the GCS object to download.
            pool_size (int): The maximum number of concurrent downloaders allowed in the pool.
        """
        self.gcsfs = gcsfs
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.generation = generation
        self.pool_size = pool_size

        self._free_mrds = asyncio.Queue(maxsize=pool_size)
        self._active_count = 0
        self._creation_lock = asyncio.Lock()
        self.persisted_size = None
        self._initialized = False
        self._all_mrds = []

    async def _create_mrd(self):
        await self.gcsfs._get_grpc_client()
        from google.cloud.storage.asyncio.async_multi_range_downloader import (
            AsyncMultiRangeDownloader,
        )

        mrd = await AsyncMultiRangeDownloader.create_mrd(
            self.gcsfs.grpc_client, self.bucket_name, self.object_name, self.generation
        )
        self._all_mrds.append(mrd)
        return mrd

    async def initialize(self):
        """Initializes the MRDPool by creating the first downloader instance."""
        async with self._creation_lock:
            if not self._initialized:
                mrd = await self._create_mrd()
                self.persisted_size = mrd.persisted_size
                self._free_mrds.put_nowait(mrd)
                self._active_count += 1
                self._initialized = True

    @contextlib.asynccontextmanager
    async def get_mrd(self):
        """
        Dynamically provisions MRDs using an async context manager.

        If a downloader is available in the pool, it is yielded immediately. If the
        pool is empty but hasn't reached `pool_size`, a new downloader is spawned
        on demand. Automatically returns the downloader to the free queue upon exit.

        Yields:
            AsyncMultiRangeDownloader: An active downloader ready for requests.

        Raises:
            Exception: Bubbles up any exceptions encountered during MRD creation.
        """
        spawn_new = False

        if self._free_mrds.empty():
            async with self._creation_lock:
                if self._active_count < self.pool_size:
                    self._active_count += 1
                    spawn_new = True

        if spawn_new:
            try:
                mrd = await self._create_mrd()
            except Exception as e:
                self._active_count -= 1
                raise e
        else:
            mrd = await self._free_mrds.get()

        try:
            yield mrd
        finally:
            self._free_mrds.put_nowait(mrd)

    async def close(self):
        """
        Cleanly shut down all MRDs.

        Iterates through all instantiated downloaders and calls their close methods
        with a 2-second timeout to prevent indefinite hanging during teardown.
        """
        tasks = []
        for mrd in self._all_mrds:
            tasks.append(mrd.close())
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)
        except Exception:
            pass
        self._all_mrds.clear()
