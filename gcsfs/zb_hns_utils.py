import asyncio
import collections
import contextlib
import ctypes
import logging
import os
import threading
import weakref
from io import BytesIO

from google.api_core.exceptions import NotFound
from google.cloud.storage.asyncio.async_appendable_object_writer import (
    _DEFAULT_FLUSH_INTERVAL_BYTES,
    AsyncAppendableObjectWriter,
)
from google.cloud.storage.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)

MRD_MAX_RANGES = 1000  # MRD supports up to 1000 ranges per request
DEFAULT_CONCURRENCY = int(os.environ.get("DEFAULT_GCSFS_CONCURRENCY", "1"))
MAX_PREFETCH_SIZE = 256 * 1024 * 1024
logger = logging.getLogger("gcsfs")


PyBytes_FromStringAndSize = ctypes.pythonapi.PyBytes_FromStringAndSize
PyBytes_FromStringAndSize.argtypes = (ctypes.c_void_p, ctypes.c_ssize_t)
PyBytes_FromStringAndSize.restype = ctypes.py_object

PyBytes_AsString = ctypes.pythonapi.PyBytes_AsString
PyBytes_AsString.argtypes = (ctypes.py_object,)
PyBytes_AsString.restype = ctypes.c_void_p


async def init_mrd(grpc_client, bucket_name, object_name, generation=None):
    """
    Creates the AsyncMultiRangeDownloader using an existing client.
    Wraps Google API errors into standard Python exceptions.
    """
    try:
        return await AsyncMultiRangeDownloader.create_mrd(
            grpc_client, bucket_name, object_name, generation
        )
    except NotFound:
        # We wrap the error here to match standard Python error handling
        # and avoid leaking Google API exceptions to users.
        raise FileNotFoundError(f"{bucket_name}/{object_name}")


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
    bytes_downloaded = len(data)

    if length != bytes_downloaded:
        logger.warning(
            f"Short read detected for {mrd.bucket_name}/{mrd.object_name}! "
            f"Requested {length} bytes but downloaded {bytes_downloaded} bytes."
        )

    logger.debug(
        f"Requested {length} bytes from offset {offset}, downloaded {bytes_downloaded} "
        f"bytes from mrd path: {mrd.bucket_name}/{mrd.object_name}"
    )
    return data


async def download_ranges(ranges, mrd):
    """
    Downloads multiple byte ranges from the file asynchronously in a single batch.

    Args:
        ranges: List of (offset, length) tuples to download. Max 1000 ranges allowed.
        mrd: AsyncMultiRangeDownloader instance

    Returns:
        List of bytes objects, one for each range
    """
    # Prepare tasks: Filter out empty ranges and create buffers immediately
    # Structure: (original_index, offset, length, buffer)
    # Calling MRD with length=0 returns till end of file. We handle zero-length
    # ranges by returning b"" without calling MRD. So only create tasks for length > 0

    if len(ranges) > MRD_MAX_RANGES:
        raise ValueError("Invalid input - number of ranges cannot be more than 1000")

    tasks = [
        (i, off, length, BytesIO())
        for i, (off, length) in enumerate(ranges)
        if length > 0
    ]

    # Execute Download
    if tasks:
        # The MRD expects list of (offset, length, buffer)
        # We extract these from our task list
        await mrd.download_ranges([(off, length, buf) for _, off, length, buf in tasks])

    # Map results back to their original positions
    results = [b""] * len(ranges)
    for i, _, _, buffer in tasks:
        results[i] = buffer.getvalue()

    # Log stats
    total_requested = sum(r[1] for r in ranges)
    total_downloaded = sum(len(r) for r in results)

    if total_requested != total_downloaded:
        logger.warning(
            f"Short read detected for {mrd.bucket_name}/{mrd.object_name}! "
            f"Requested {total_requested} bytes but downloaded {total_downloaded} bytes."
        )

    if logger.isEnabledFor(logging.DEBUG):
        requested_ranges_to_log = [(r[0], r[1]) for r in ranges]
        logger.debug(
            f"mrd path: {mrd.bucket_name}/{mrd.object_name} | "
            f"Requested {len(ranges)} ranges: {requested_ranges_to_log} | "
            f"total bytes requested: {total_requested} | "
            f"total bytes downloaded: {total_downloaded}"
        )

    return results


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


async def close_mrd(mrd):
    """
    Closes the AsyncMultiRangeDownloader gracefully.
    Logs a warning if closing fails, instead of raising an exception.
    """
    if mrd:
        try:
            await mrd.close()
        except Exception as e:
            logger.warning(
                f"Error closing AsyncMultiRangeDownloader for {mrd.bucket_name}/{mrd.object_name}: {e}"
            )


async def close_aaow(aaow, finalize_on_close=False):
    """
    Closes the AsyncAppendableObjectWriter gracefully.
    Logs a warning if closing fails, instead of raising an exception.
    """
    if aaow:
        try:
            await aaow.close(finalize_on_close=finalize_on_close)
        except Exception as e:
            logger.warning(
                f"Error closing AsyncAppendableObjectWriter for {aaow.bucket_name}/{aaow.object_name}: {e}"
            )


class DirectMemmoveBuffer:
    """
    A buffer-like object that writes data directly to memory asynchronously.

    This class provides a `write` interface that queues `ctypes.memmove` operations
    to a thread pool executor, limiting the maximum number of concurrent pending
    writes using a semaphore. It is useful for high-performance data transfers
    where memory copies need to be offloaded from the main thread.
    """

    def __init__(self, start_address, end_address, executor, max_pending=5):
        """
        Initializes the DirectMemmoveBuffer.

        Args:
            start_address (int): The starting memory address where data will be written.
            end_address (int): The absolute ending memory address. Writes exceeding
                this boundary will be rejected to prevent overflows.
            executor (concurrent.futures.Executor): The thread pool executor to run the
                memmove operations. The lifecycle of this executor is managed by the caller.
            max_pending (int, optional): The maximum number of pending write operations
                allowed in the queue. Defaults to 5.
        """
        self.start_address = start_address
        self.end_address = end_address
        self.executor = executor

        # Volatile state variables. Must only be amended while holding self._lock.
        self.current_offset = 0
        self._pending_count = 0
        self._error = None

        # Primitives:
        # 1. semaphore: Provides backpressure by limiting the number of active tasks.
        # 2. _lock: Protects mutations to the volatile state variables above.
        # 3. _done_event: Signals when the queue of active background tasks reaches zero.
        self.semaphore = threading.Semaphore(max_pending)
        self._lock = threading.Lock()
        self._done_event = threading.Event()
        self._done_event.set()

    def _decrement_pending(self):
        """Helper to cleanly release concurrency primitives after a task finishes."""
        self.semaphore.release()
        with self._lock:
            self._pending_count -= 1
            if self._pending_count == 0:
                self._done_event.set()

    def write(self, data):
        """
        Schedules a write operation to memory.

        Calculates the destination address based on the current offset, increments the offset,
        and submits the memory move operation to the executor. Blocks if the number of
        pending operations reaches `max_pending`.

        Args:
            data: The data to be written to memory. Must support the buffer protocol.

        Returns:
            concurrent.futures.Future: A future object representing the execution of the
                memory move operation.

        Raises:
            Exception: If any previous asynchronous write operation encountered an error.
            BufferError: If the write exceeds the allocated memory boundaries.
        """
        if self._error:
            raise self._error

        size = len(data)
        with self._lock:
            dest = self.start_address + self.current_offset
            if dest + size > self.end_address:
                error_msg = (
                    f"Attempted to write {size} bytes "
                    f"at offset {self.current_offset}. "
                    f"Max capacity is {self.end_address - self.start_address} bytes."
                )
                raise BufferError(error_msg)

            self.current_offset += size
        data_bytes = bytes(data) if not isinstance(data, bytes) else data

        self.semaphore.acquire()
        with self._lock:
            if self._pending_count == 0:
                self._done_event.clear()
            self._pending_count += 1

        try:
            return self.executor.submit(self._do_memmove, dest, data_bytes, size)
        except BaseException as e:
            self._error = e
            self._decrement_pending()
            raise e

    def _do_memmove(self, dest, data_bytes, size):
        try:
            ctypes.memmove(dest, data_bytes, size)
        except Exception as e:
            with self._lock:
                self._error = e
            raise e
        finally:
            self._decrement_pending()

    def close(self):
        """
        Waits for all pending write operations to complete and checks for errors.
        Blocks the calling thread until the queue of memory operations is entirely
        processed.

        Raises:
            Exception: If any background write operation failed during execution.
            BufferError: If the buffer was not filled to the expected capacity.
        """
        self._done_event.wait()
        if self._error:
            raise self._error

        expected_size = self.end_address - self.start_address
        if self.current_offset < expected_size:
            error_msg = (
                f"Expected {expected_size} bytes, "
                f"but only received {self.current_offset} bytes. "
                f"Buffer contains uninitialized data."
            )
            raise BufferError(error_msg)


def _drain_queue(q):
    """Synchronously extract all items from an asyncio.Queue. Returns an empty list if q is None."""
    if q is None:
        return []
    items = []
    while not q.empty():
        try:
            items.append(q.get_nowait())
        except asyncio.QueueEmpty:
            break
    return items


async def _close_mrds(mrds, raise_exception=False):
    """Close a list of MRDs asynchronously."""
    if not mrds:
        return
    results = await asyncio.gather(
        *(mrd.close() for mrd in mrds), return_exceptions=True
    )
    for r in results:
        if isinstance(r, Exception):
            if raise_exception:
                raise r
            logger.warning("Error closing MRD: %s", r)


class MRDPool:
    """Manages a pool of AsyncMultiRangeDownloader objects with on-demand scaling.

    When constructed by `MRDPoolCache`, the instance acts as a pool over a shared
    MRD queue and donates its MRDs back to that queue on close. Direct
    construction keeps the original standalone lifecycle and closes its MRDs.
    """

    def __init__(
        self,
        gcsfs,
        bucket_name,
        object_name,
        generation,
        pool_size,
        cache=None,
    ):
        self.gcsfs = gcsfs
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.generation = generation
        self._cache = cache
        self._key = (bucket_name, object_name, generation)
        self.pool_size = pool_size
        self._free_mrds = asyncio.Queue(maxsize=pool_size)
        self._active_count = 0
        self._lock = asyncio.Lock()
        self.persisted_size = None
        self._initialized = False
        self._closed = False
        self._all_mrds = []
        self._rr_index = 0

        # Change this to true once MRD supports concurrent requests.
        self.mrd_supports_multi_request = False

    async def _create_mrd(self):
        await self.gcsfs._get_grpc_client()
        mrd = await init_mrd(
            self.gcsfs.grpc_client,
            self.bucket_name,
            self.object_name,
            self.generation,
        )
        self._all_mrds.append(mrd)
        return mrd

    async def _get_or_create_mrd(self):
        """Gets an MRD from the MRD queue or creates a new one."""
        mrd = None

        if self._cache is not None:
            mrd = await self._cache.get_idle_mrd(self._key)
            if mrd is not None:
                self._all_mrds.append(mrd)

        if mrd is None:
            mrd = await self._create_mrd()

        return mrd

    async def initialize(self):
        """Initializes the MRDPool by creating the first downloader instance."""
        async with self._lock:
            if self._closed:
                raise RuntimeError("Cannot initialize a closed MRDPool.")

            if not self._initialized and self._active_count == 0:
                mrd = await self._get_or_create_mrd()
                self.persisted_size = mrd.persisted_size
                self._free_mrds.put_nowait(mrd)
                self._active_count += 1

            self._initialized = True

    @contextlib.asynccontextmanager
    async def get_mrd(self):
        """Dynamically provisions MRDs using an async context manager."""
        create_new = False
        used_from_queue = False
        mrd = None

        async with self._lock:
            if self._closed:
                raise RuntimeError("MRDPool is closed")

            if self._free_mrds.empty():
                if self._active_count < self.pool_size:
                    self._active_count += 1
                    create_new = True
                elif self.mrd_supports_multi_request and self._all_mrds:
                    mrd = self._all_mrds[self._rr_index]
                    self._rr_index = (self._rr_index + 1) % len(self._all_mrds)

            if create_new:
                try:
                    mrd = await self._get_or_create_mrd()
                except BaseException as e:
                    self._active_count -= 1
                    raise e
            elif mrd is None:
                mrd = await self._free_mrds.get()
                used_from_queue = True

        try:
            yield mrd
        finally:
            # Only return the MRD to the free queue if we were the ones who took it out
            # or if we just spawned it. This prevents duplicate entries in the queue
            # when multiple concurrent tasks share the same MRD via round-robin.
            if (create_new or used_from_queue) and not self._closed:
                self._free_mrds.put_nowait(mrd)

    async def close(self):
        async with self._lock:
            if self._closed:
                return

            if self._initialized and self._free_mrds.qsize() != len(self._all_mrds):
                raise RuntimeError("Cannot close pool with active MRDs")

            try:
                if self._cache is not None:
                    await self._cache.release(self._key, self._all_mrds)
                else:
                    await _close_mrds(self._all_mrds, raise_exception=True)
            finally:
                self._all_mrds.clear()
                self._closed = True


class MRDPoolCache:
    """FS-level cache of MRD queues keyed by
    (bucket, object, generation). Owned by `ExtendedGcsFileSystem`.

    Lifecycle: `get` returns an `MRDPool` instance; its `close` calls
    back into `release`, which decrements refcount and, when it hits zero,
    appends the key to an LRU of evictable keys. Pools beyond
    `max_idle_pools` are evicted and torn down.
    """

    def __init__(self, gcsfs, max_idle_pools: int = 16):
        self._gcsfs = weakref.ref(gcsfs)
        self._max_idle_pools = max_idle_pools
        self._mrd_queues: dict = {}
        self._refcounts: dict = {}
        self._evictable_keys: "collections.OrderedDict" = collections.OrderedDict()
        self._lock = asyncio.Lock()
        self._closed = False

    async def get_idle_mrd(self, key):
        """Gets an MRD from the queue for the given key."""
        async with self._lock:
            if self._closed:
                return None
            queue = self._mrd_queues.get(key)
            if queue is not None:
                try:
                    return queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
        return None

    async def get(self, bucket_name, object_name, generation, pool_size):
        fs = self._gcsfs()
        if fs is None:
            raise RuntimeError("ExtendedGcsFileSystem has been garbage collected")

        if generation is None:
            info = await fs._info(f"{bucket_name}/{object_name}")
            generation = info.get("generation")

        key = (bucket_name, object_name, generation)
        async with self._lock:
            if self._closed:
                raise RuntimeError("MRDPoolCache is closed")
            mrd_queue = self._mrd_queues.get(key)
            if mrd_queue is None:
                mrd_queue = asyncio.Queue()
                self._mrd_queues[key] = mrd_queue
            self._refcounts[key] = self._refcounts.get(key, 0) + 1
            if key in self._evictable_keys:
                del self._evictable_keys[key]

        mrd_pool = MRDPool(
            fs,
            bucket_name,
            object_name,
            generation,
            pool_size,
            cache=self,
        )

        try:
            await mrd_pool.initialize()
        except BaseException:
            await mrd_pool.close()
            mrds_to_close = []
            async with self._lock:
                if key not in self._refcounts:
                    if key in self._evictable_keys:
                        del self._evictable_keys[key]
                    q = self._mrd_queues.pop(key, None)
                    mrds_to_close.extend(_drain_queue(q))
            await _close_mrds(mrds_to_close, raise_exception=False)
            raise

        return mrd_pool

    async def release(self, key, mrds):
        mrds_to_close = []
        async with self._lock:
            mrd_queue = self._mrd_queues.get(key)

            if mrd_queue is not None:
                for mrd in mrds:
                    mrd_queue.put_nowait(mrd)
            else:
                mrds_to_close.extend(mrds)

            refcount = self._refcounts.get(key, 0) - 1
            if refcount <= 0:
                self._refcounts.pop(key, None)
                if not self._closed:
                    self._evictable_keys[key] = None
                    while len(self._evictable_keys) > self._max_idle_pools:
                        evict_key, _ = self._evictable_keys.popitem(last=False)
                        q = self._mrd_queues.pop(evict_key, None)
                        mrds_to_close.extend(_drain_queue(q))
            else:
                self._refcounts[key] = refcount

        await _close_mrds(mrds_to_close, raise_exception=False)

    async def close(self):
        async with self._lock:
            if self._closed:
                return

            mrds_to_close = []
            for q in self._mrd_queues.values():
                mrds_to_close.extend(_drain_queue(q))
            try:
                await _close_mrds(mrds_to_close, raise_exception=True)
            finally:
                self._mrd_queues = {}
                self._refcounts = {}
                self._evictable_keys.clear()
                self._closed = True
