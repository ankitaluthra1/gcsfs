"""Worker module for interpreter-parallel HTTP prefetch (standard buckets).

Imported by BARE name (`import interp_worker`), NOT as `gcsfs.interp_worker`.
That is deliberate: loading it inside a worker subinterpreter must not execute
`gcsfs/__init__.py` (which pulls in aiohttp and the full stack). Every import
here is stdlib and subinterpreter-safe.

Each worker interpreter downloads a byte range with a ranged HTTP GET and
memmoves it once into a shared-memory block. Nothing but ints, a shm name and a
bearer token ever crosses the interpreter boundary.
"""

import http.client
import urllib.parse
from multiprocessing import shared_memory

# Per-interpreter state, set once by worker_init and reused by worker_fetch.
_STATE: dict = {}


class ShmRangeWriter:
    """A `write()`-only sink that memmoves bytes into a fixed slice of a
    shared-memory block. The HTTP response body is streamed through here, so the
    payload is copied exactly once (socket buffer -> shared memory)."""

    __slots__ = ("_buf", "_base", "_cap", "_cursor")

    def __init__(self, buf, dest_off, capacity):
        self._buf = buf          # memoryview over the shared-memory block
        self._base = dest_off
        self._cap = capacity
        self._cursor = 0

    def write(self, data):
        n = len(data)
        if self._cursor + n > self._cap:
            raise BufferError(f"write of {n} bytes exceeds range capacity {self._cap}")
        start = self._base + self._cursor
        self._buf[start : start + n] = data
        self._cursor += n
        return n


def _new_conn():
    if _STATE["scheme"] == "https":
        import ssl

        return http.client.HTTPSConnection(
            _STATE["host"], timeout=60, context=ssl.create_default_context()
        )
    return http.client.HTTPConnection(_STATE["host"], timeout=60)


def worker_init(bucket, object_name, generation, token, host, scheme):
    """Run once per worker interpreter (pool initializer). Stashes the request
    config; the connection is created lazily and kept alive across fetches.

    Uses the XML-API media endpoint: `{scheme}://{host}/{bucket}/{object}`.
    """
    path = "/" + bucket + "/" + urllib.parse.quote(object_name, safe="/")
    if generation:
        path += f"?generation={generation}"
    _STATE.update(token=token, host=host, scheme=scheme, path=path, conn=None)


def worker_fetch(offset, length, shm_name, dest_off):
    """Download `[offset, offset+length)` into the shared-memory block. Reuses a
    keep-alive connection per worker; reconnects once on a connection error."""
    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
    if _STATE.get("token"):
        headers["Authorization"] = "Bearer " + _STATE["token"]

    # track=False: the main interpreter created and owns this segment (and
    # unlinks it). Without this each worker's resource_tracker would also unlink
    # it, causing premature unlink / "leaked shared_memory" warnings.
    shm = shared_memory.SharedMemory(name=shm_name, track=False)
    buf = memoryview(shm.buf)
    try:
        writer = ShmRangeWriter(buf, dest_off, length)
        for attempt in range(2):
            conn = _STATE.get("conn") or _new_conn()
            _STATE["conn"] = conn
            try:
                conn.request(method="GET", url=_STATE["path"], headers=headers)
                resp = conn.getresponse()
                if resp.status not in (200, 206):
                    body = resp.read(512)
                    raise RuntimeError(f"HTTP {resp.status} for {_STATE['path']}: {body!r}")
                while True:
                    chunk = resp.read(1 << 16)
                    if not chunk:
                        break
                    writer.write(chunk)  # single memmove into shared memory
                return writer._cursor
            except (http.client.HTTPException, OSError):
                try:
                    conn.close()
                finally:
                    _STATE["conn"] = None
                if attempt == 1:
                    raise
    finally:
        buf.release()
