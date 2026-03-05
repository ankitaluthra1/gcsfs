import asyncio
from collections import deque

import fsspec.asyn
from fsspec.caching import BaseCache, register_cache


class ReadAheadChunked(BaseCache):
    """
    An optimized ReadAhead cache that fetches multiple chunks in a single
    HTTP request but manages them as separate bytes objects to avoid
    expensive memory slicing.

    While this approach primarily optimizes for CPU and memory allocation overhead,
    it strictly maintains the same semantics as the existing readahead cache.
    For example, if a user requests 5MB and the cache fetches 10MB, it serves the
    requested 5MB but retains that data in memory to handle potential backward seeks.
    This mirrors the standard readahead behavior, which does not eagerly discard served
    chunks until a new fetch is required.
    """

    name = "readahead_chunked"

    def __init__(self, blocksize: int, fetcher, size: int) -> None:
        super().__init__(blocksize, fetcher, size)
        self.chunks = deque()  # Entries: (start, end, data_bytes)

    @property
    def cache(self):
        """
        Compatibility property for tests/legacy code that expects 'cache'
        to be a single bytestring.

        WARNING: Accessing this property forces a memory copy of the
        entire current buffer, negating the Zero-Copy optimization
        of ReadAheadChunked. Use for debugging/testing only.
        """
        if not self.chunks:
            return b""
        return b"".join(chunk[2] for chunk in self.chunks)

    def _fetch(self, start: int | None, end: int | None) -> bytes:
        if start is None:
            start = 0
        if end is None or end > self.size:
            end = self.size
        if start >= self.size:
            return b""

        # Handle backward seeks that go beyond the start of our cache window
        if self.chunks and self.chunks[0][0] > start:
            self.chunks.clear()

        parts = []
        current_pos = start

        # Satisfy as much as possible from the existing cache (Zero-Copy)
        for c_start, c_end, c_data in self.chunks:
            if c_end <= start:
                continue  # Skip chunks completely before our window

            if c_start >= end:
                break  # If we've reached chunks completely past our window, stop

            if c_end > current_pos:
                slice_start = max(0, current_pos - c_start)
                slice_end = min(len(c_data), end - c_start)

                if slice_start == 0 and slice_end == len(c_data):
                    # Zero-copy: Direct reference to the full object
                    parts.append(c_data)
                else:
                    # Slicing creates a copy, but it's unavoidable for partials
                    parts.append(c_data[slice_start:slice_end])

                current_pos += slice_end - slice_start

        # Fetch missing data if necessary
        should_fetch_backend = current_pos < end
        if should_fetch_backend:
            # On a cache miss, we replace the entire window (standard readahead behavior)
            self.chunks.clear()

            missing_len = min(self.size - current_pos, end - current_pos)
            readahead_block = min(
                self.size - (current_pos + missing_len), self.blocksize
            )

            self.miss_count += 1
            chunk_lengths = [missing_len]
            if readahead_block > 0:
                chunk_lengths.append(readahead_block)

            # Vector read call
            new_chunks = self.fetcher(start=current_pos, chunk_lengths=chunk_lengths)

            # Process the requested data
            req_data = new_chunks[0]
            self.chunks.append((current_pos, current_pos + len(req_data), req_data))
            self.total_requested_bytes += len(req_data)
            parts.append(req_data)

            # Process the readahead data (if any)
            if len(new_chunks) > 1:
                ra_data = new_chunks[1]
                ra_start = current_pos + len(req_data)
                self.chunks.append((ra_start, ra_start + len(ra_data), ra_data))
                self.total_requested_bytes += len(ra_data)

        if not parts:
            return b""

        if not should_fetch_backend:
            self.hit_count += 1

        # Optimization: return the single object directly if possible
        if len(parts) == 1:
            return parts[0]

        return b"".join(parts)


class Prefetcher(BaseCache):
    """
    Asynchronous prefetching cache that reads ahead.

    This cache spawns a background producer task that fetches sequential
    blocks of data before they are explicitly requested. It is highly optimized
    for sequential reads but can recover from arbitrary seeks by restarting
    the prefetch loop.

    Parameters
    ----------
    blocksize : int
        Base size of the chunks to read ahead, in bytes.
    fetcher : Callable
        A coroutine of the form `f(start, end)` which gets bytes from the remote.
    size : int
        Total size of the file being read.
    max_prefetch_size : int, optional
        Maximum bytes to prefetch ahead of the current user offset.
        Defaults to `max(2 * blocksize, 128MB)`.
    concurrency : int, optional
        Number of concurrent network requests to use for large chunks. Defaults to 4.
    """

    name = "prefetcher"

    MIN_CHUNK_SIZE = 16 * 1024 * 1024
    DEFAULT_PREFETCH_SIZE = 128 * 1024 * 1024

    def __init__(
        self,
        blocksize: int,
        fetcher,
        size: int,
        max_prefetch_size=None,
        concurrency=4,
        **kwargs,
    ):
        super().__init__(blocksize, fetcher, size)
        self.fetcher = kwargs.pop("fetcher_override", self.fetcher)
        self.concurrency = concurrency
        self.max_prefetch_size = max(
            max_prefetch_size or 0, 2 * self.blocksize, self.DEFAULT_PREFETCH_SIZE
        )

        self.sequential_streak = 0
        self.user_offset = 0
        self.current_offset = 0
        self.queue = asyncio.Queue()
        self.is_stopped = False
        self._active_tasks = set()
        self._wakeup_producer = asyncio.Event()
        self._remainder = b""
        self._buffer_offset = 0
        self.loop = fsspec.asyn.get_loop()

        async def _start_producer():
            self._producer_task = asyncio.create_task(self._producer_loop())

        fsspec.asyn.sync(self.loop, _start_producer)

    async def _cancel_all_tasks(self, wait=False):
        self.is_stopped = True
        self._wakeup_producer.set()

        tasks_to_wait = []

        if hasattr(self, "_producer_task") and isinstance(
            self._producer_task, asyncio.Task
        ):
            if not self._producer_task.done():
                self._producer_task.cancel()
                tasks_to_wait.append(self._producer_task)

        for task in list(self._active_tasks):
            if not task.done():
                tasks_to_wait.append(task)

        self._active_tasks.clear()
        if hasattr(self, "queue"):
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

        if wait and tasks_to_wait:
            await asyncio.gather(*tasks_to_wait, return_exceptions=True)

    async def _restart_producer(self):
        # Cancel old tasks without waiting
        await self._cancel_all_tasks(wait=False)
        self.is_stopped = False
        self.sequential_streak = 0
        self._producer_task = asyncio.create_task(self._producer_loop())

    async def _producer_loop(self):
        try:
            while not self.is_stopped:
                await self._wakeup_producer.wait()
                self._wakeup_producer.clear()

                if self.is_stopped:
                    break

                prefetch_size = min(
                    (self.sequential_streak + 1) * self.blocksize,
                    self.max_prefetch_size,
                )

                while (
                    self.current_offset - self.user_offset
                ) < prefetch_size and self.current_offset < self.size:
                    actual_size = min(self.blocksize, self.size - self.current_offset)
                    if self.sequential_streak < 2:
                        sfactor = (
                            self.concurrency
                            if actual_size >= self.MIN_CHUNK_SIZE
                            else min(self.concurrency, 2)
                        )  # random usecase
                    else:
                        sfactor = (
                            min(
                                self.concurrency,
                                max(1, actual_size // self.MIN_CHUNK_SIZE),
                            )
                            if actual_size >= self.MIN_CHUNK_SIZE
                            else 1
                        )  # sequential usecase

                    download_task = asyncio.create_task(
                        self.fetcher(
                            self.current_offset, actual_size, split_factor=sfactor
                        )
                    )
                    self._active_tasks.add(download_task)
                    download_task.add_done_callback(self._active_tasks.discard)

                    await self.queue.put(download_task)
                    self.current_offset += actual_size

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await self.queue.put(e)
            self.is_stopped = True

    async def read(self):
        """Reads the next chunk from the object."""
        if self.user_offset >= self.size:
            return b""
        if self.is_stopped and self.queue.empty():
            return b""

        if self.queue.empty():
            self._wakeup_producer.set()

        task = await self.queue.get()

        # Check if the producer pushed an exception
        if isinstance(task, Exception):
            self.is_stopped = True
            raise task

        if task.done():
            self.hit_count += 1
        else:
            self.miss_count += 1

        try:
            block = await task
            self.user_offset += len(block)
            self.sequential_streak += 1
            if self.sequential_streak >= 2:
                self._wakeup_producer.set()  # starts prefetching.
            return block
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.is_stopped = True
            raise e

    async def seek(self, new_offset):
        if new_offset == self.user_offset:
            return

        self.user_offset = new_offset
        self.current_offset = new_offset
        await self._restart_producer()

    async def _async_fetch(self, start, end):
        if start != self._buffer_offset:
            self._remainder = b""
            self._buffer_offset = start
            await self.seek(start)

        requested_size = end - start
        chunks = []
        collected = 0

        # Take any leftover bytes from a previous misaligned fetch
        if self._remainder:
            chunks.append(self._remainder)
            collected += len(self._remainder)

        while collected < requested_size and self.user_offset < self.size:
            block = await self.read()
            if not block:
                break
            chunks.append(block)
            collected += len(block)

        if len(chunks) == 1 and len(chunks[0]) == requested_size:
            out = chunks[0]
            self._remainder = b""
        else:
            full_data = b"".join(chunks)
            out = full_data[:requested_size]
            self._remainder = full_data[requested_size:]

        self._buffer_offset += len(out)

        self.total_requested_bytes += len(out)
        return out

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        if start is None:
            start = 0
        if stop is None:
            stop = self.size
        if start >= self.size or start >= stop:
            return b""
        return fsspec.asyn.sync(self.loop, self._async_fetch, start, stop)

    def close(self):
        """Clean shutdown. Cancels tasks and waits for them to abort."""
        fsspec.asyn.sync(self.loop, self._cancel_all_tasks, True)


for gcs_cache in [ReadAheadChunked, Prefetcher]:
    register_cache(gcs_cache, clobber=True)
