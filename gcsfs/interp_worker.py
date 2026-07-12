import asyncio
from multiprocessing import shared_memory

# Per-interpreter state, populated once by worker_init and reused by worker_fetch.
_STATE: dict = {}


class ShmRangeWriter:
    """A minimal `write()`-only sink that memmoves incoming bytes into a fixed
    slice of a shared-memory block. The GCS async MRD calls `buffer.write(data)`
    sequentially. This performs the single payload copy into shared memory so no
    bytes ever cross the interpreter boundary.
    """

    __slots__ = ("_buf", "_base", "_cap", "_cursor")

    def __init__(self, buf, dest_off: int, capacity: int):
        self._buf = buf            # memoryview over the shared-memory block
        self._base = dest_off
        self._cap = capacity
        self._cursor = 0

    def write(self, data) -> int:
        n = len(data)
        if self._cursor + n > self._cap:
            raise BufferError(
                f"write of {n} bytes exceeds range capacity {self._cap} "
                f"(cursor={self._cursor})"
            )

        start = self._base + self._cursor
        self._buf[start : start + n] = data  # single memmove into shared memory
        self._cursor += n
        return n

    @property
    def written(self) -> int:
        return self._cursor


def worker_init(bucket: str, object_name: str, generation, token: str) -> None:
    """Run once per worker interpreter (as the InterpreterPoolExecutor
    initializer). Opens one AsyncMultiRangeDownloader on a persistent event loop
    and caches it. Expensive channel/bidi setup is amortized across all fetches.

    Credentials are a pure bearer token, so no service-account signing /
    ``cryptography`` hazmat import happens inside the subinterpreter.
    """
    # Imports live here so the subinterpreter loads grpc/storage under its own
    # GIL. If grpc cannot be imported in an own-GIL subinterpreter, it fails here.
    import grpc # noqa: F401
    from google.oauth2.credentials import Credentials
    from google.cloud.storage.asyncio.async_grpc_client import AsyncGrpcClient
    from google.cloud.storage.asyncio.async_multi_range_downloader import (
        AsyncMultiRangeDownloader,
    )

    loop = asyncio.new_event_loop()
    creds = Credentials(token=token) # bearer only; no refresh, no signing
    client = AsyncGrpcClient(credentials=creds, attempt_direct_path=False)
    mrd = loop.run_until_complete(
        AsyncMultiRangeDownloader.create_mrd(client, bucket, object_name, generation)
    )
    _STATE.update(loop=loop, mrd=mrd, client=client)


def worker_fetch(abs_offset: int, length: int, shm_name: str, dest_off: int) -> int:
    """Download one sub-range into the shared-memory block via the cached MRD.

    Returns the number of bytes written. Runs on this interpreter's own GIL, so
    the grpc response deserialization + crc32c + memmove proceed in parallel with
    other worker interpreters.
    """
    loop = _STATE["loop"]
    mrd = _STATE["mrd"]
    shm = shared_memory.SharedMemory(name=shm_name)
    buf = memoryview(shm.buf)
    try:
        writer = ShmRangeWriter(buf, dest_off, length)
        loop.run_until_complete(mrd.download_ranges([(abs_offset, length, writer)]))
        return writer.written
    finally:
        buf.release()
        shm.close() # detach only; the main interpreter owns unlink()