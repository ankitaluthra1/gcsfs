"""Interpreter-parallel HTTP prefetch fetcher for gcsfs standard buckets.

Replaces the prefetcher's asyncio ``concurrency`` (N cooperative coroutines under
one GIL) with N own-GIL worker interpreters (PEP 684 / 734, CPython >= 3.14).
Each worker downloads a byte range via a stdlib ranged GET and memmoves it once
into a ``multiprocessing.shared_memory`` block, so the range downloads run with
true parallelism. Only ints, a shm name and a bearer token cross the interpreter
boundary. Feature-flagged; default off.
"""

import asyncio
import logging
import os
import sys
import urllib.parse
from multiprocessing import shared_memory

from gcsfs.concurrency import split_range

logger = logging.getLogger("gcsfs.interpreter_prefetch")

MIN_PYTHON = (3, 14)
MIN_CHUNK_SIZE = 5 * 1024 * 1024
_TRUTHY = {"1", "true", "yes"}

def _require_py314():
    if sys.version_info < MIN_PYTHON:
        raise RuntimeError(
            "Interpreter-parallel prefetch requires CPython >= 3.14 "
            f"(have {sys.version_info.major}.{sys.version_info.minor}). "
            "Unset USE_INTERPRETER_PARALLEL_PREFETCH to use the asyncio path."
        )

def interpreter_prefetch_enabled(kwarg_value=None):
    """True iff the interpreter-parallel path is requested via kwarg or env."""
    if kwarg_value:
        return True
    return os.environ.get("USE_INTERPRETER_PARALLEL_PREFETCH", "").lower() in _TRUTHY

def _http_endpoint():
    """Resolve ``(host, scheme)`` for downloads; honors STORAGE_EMULATOR_HOST so
    the path can also run against a local HTTP emulator."""
    emu = os.environ.get("STORAGE_EMULATOR_HOST")
    if emu:
        parsed = urllib.parse.urlparse(emu if "://" in emu else "http://" + emu)
        return (parsed.netloc or parsed.path), (parsed.scheme or "http")
    return "storage.googleapis.com", "https"

def _import_worker():
    """Import the worker module by BARE name so worker subinterpreters do not
    execute ``gcsfs/__init__.py`` (which would pull in aiohttp and the full
    stack). The file lives in this package's directory; we add that directory to
    ``sys.path`` and import it top-level."""
    pkg_dir = os.path.dirname(__file__)
    if pkg_dir not in sys.path:
        sys.path.insert(0, pkg_dir)
    import interp_worker  # noqa: E402  (intentional top-level, non-package import)

    return interp_worker

class InterpreterParallelFetcher:
    """Prefetcher leaf fetcher backed by a pool of own-GIL worker interpreters.

    One pool per open file. The bearer token is fetched once from
    ``token_provider`` when the pool is created and handed to each worker.
    Matches the prefetcher's fetcher contract: ``async (start, size, split_factor)
    -> bytes``.
    """

    def __init__(
        self,
        bucket,
        object_name,
        generation,
        size,
        token_provider,
        workers,
        min_chunk_size=MIN_CHUNK_SIZE,
    ):
        _require_py314()
        self.bucket = bucket
        self.object = object_name
        self.generation = generation
        self.size = size
        self.token_provider = token_provider
        self.workers = max(1, int(workers))
        self.min_chunk_size = min_chunk_size
        self._executor = None
        self._worker = None

    def _ensure_executor(self):
        if self._executor is None:
            from concurrent.futures import InterpreterPoolExecutor

            self._worker = _import_worker()
            host, scheme = _http_endpoint()
            self._executor = InterpreterPoolExecutor(
                max_workers=self.workers,
                initializer=self._worker.worker_init,
                initargs=(
                    self.bucket,
                    self.object,
                    self.generation,
                    self.token_provider(),
                    host,
                    scheme,
                ),
            )
            logger.debug(
                "InterpreterParallelFetcher: pool of %d worker interpreters for %s/%s",
                self.workers,
                self.bucket,
                self.object,
            )
        return self._executor

    async def __call__(self, start, size, split_factor=1):
        if size <= 0:
            return b""
        executor = self._ensure_executor()
        worker = self._worker
        shm = shared_memory.SharedMemory(create=True, size=size)
        try:
            # split_range returns (relative_offset, length); relative_offset is
            # also the destination offset within the size-`size` shm block.
            futures = [
                executor.submit(
                    worker.worker_fetch, start + rel, length, shm.name, rel
                )
                for rel, length in split_range(size, split_factor, self.min_chunk_size)
            ]
            # Bridge the executor's concurrent.futures.Futures onto the running
            # (fsspec background) event loop the prefetch producer awaits on.
            await asyncio.gather(*(asyncio.wrap_future(f) for f in futures))
            return bytes(shm.buf[:size])
        finally:
            shm.close()
            shm.unlink()

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None
