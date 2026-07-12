"""Interpreter-parallel prefetch fetcher for gcsfs (POC).

Fetches zonal-object sub-ranges across own-GIL subinterpreters (PEP 684 / 734,
CPython >= 3.14) so that the grpc response deserialization + crc32c + memmove
assembly run with true parallelism instead of being serialized under one GIL.

Design: keep the prefetcher's `async (start, size, split_factor) -> bytes`
fetcher contract unchanged; only swap the leaf implementation. Each worker
interpreter owns an event loop + grpc channel + AsyncMultiRangeDownloader and
memmoves its sub-range once into a `multiprocessing.shared_memory` block. Only
`(int, int, str, int)` and a bearer-token string cross the interpreter
boundary -- the payload never does.

See docs/superpowers/specs/2026-07-11-interpreter-parallel-prefetch-design.md.
Feature-flagged; default off.
"""

import asyncio
import logging
import os
import sys

from multiprocessing import shared_memory

from gcsfs.concurrency import split_range

logger = logging.getLogger("gcsfs.interpreter_prefetch")


MIN_PYTHON = (3, 14)
MIN_CHUNK_SIZE = 5 * 1024 * 1024
_TRUTHY = {"1", "true", "yes"}


def _require_py314() -> None:
    if sys.version_info < MIN_PYTHON:
        raise RuntimeError(
            "Interpreter-parallel prefetch requires CPython >= 3.14 "
            f"(have {sys.version_info.major}.{sys.version_info.minor}). "
            "Unset USE_INTERPRETER_PARALLEL_PREFETCH to use the asyncio path."
        )


def interpreter_prefetch_enabled(kwarg_value: bool | None = None) -> bool:
    """True iff the interpreter-parallel path is requested via kwarg or env."""
    if kwarg_value:
        return True
    return os.environ.get("USE_INTERPRETER_PARALLEL_PREFETCH", "").lower() in _TRUTHY


def plan_subranges(start: int, size: int, split_factor: int, min_chunk_size: int):
    """Split `[start, start+size)` into `(abs_offset, length, dest_off)`
    tuples, where `dest_off` is the position within the size-`size` shared
    buffer. The tuples tile `[0, size)` contiguously with no gaps/overlap."""
    out = []
    for rel_off, length in split_range(size, split_factor, min_chunk_size):
        out.append((start + rel_off, length, rel_off))
    return out


def _import_worker():
    """Import the standalone worker module by BARE name so worker subinterpreters
    do not execute `gcsfs/__init__.py` (which would pull in aiohttp and the
    full stack). The file lives in this package's directory; we add that
    directory to `sys.path` and import it top-level."""
    pkg_dir = os.path.dirname(__file__)
    if pkg_dir not in sys.path:
        sys.path.insert(0, pkg_dir)
    import interp_worker  # noqa: E402  (intentional top-level, non-package import)

    return interp_worker


class InterpreterParallelFetcher:
    """Drop-in replacement for the zonal `_async_fetch_range` fetcher.

    One executor (== one pool of own-GIL worker interpreters) per open file. The
    bearer token is fetched once from `token_provider` when the executor is
    created and handed to each worker's `worker_init`.
    """

    def __init__(
        self,
        bucket: str,
        object_name: str,
        generation,
        size: int,
        token_provider,
        workers: int,
        min_chunk_size: int = MIN_CHUNK_SIZE,
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
            token = self.token_provider()
            self._executor = InterpreterPoolExecutor(
                max_workers=self.workers,
                initializer=self._worker.worker_init,
                initargs=(self.bucket, self.object, self.generation, token),
            )
            logger.debug(
                "InterpreterParallelFetcher: pool of %d worker interpreters for %s/%s",
                self.workers,
                self.bucket,
                self.object,
            )
        return self._executor

    async def __call__(self, start: int, size: int, split_factor: int = 1) -> bytes:
        if size <= 0:
            return b""

        executor = self._ensure_executor()
        worker = self._worker

        plan = plan_subranges(start, size, split_factor, self.min_chunk_size)
        shm = shared_memory.SharedMemory(create=True, size=size)
        try:
            futures = [
                executor.submit(
                    worker.worker_fetch, abs_off, length, shm.name, dest_off
                )
                for (abs_off, length, dest_off) in plan
            ]
            # Bridge the executor's concurrent.futures.Futures onto the running
            # (fsspec background) event loop the prefetch producer awaits on.
            await asyncio.gather(*(asyncio.wrap_future(f) for f in futures))
            # POC: one same-interpreter materialization to honor the bytes-returning
            # contract. Removable via a memoryview-returning consumer (Approach 2).
            return bytes(shm.buf[:size])
        finally:
            shm.close()
            shm.unlink()

    def close(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=True)
            self._executor = None