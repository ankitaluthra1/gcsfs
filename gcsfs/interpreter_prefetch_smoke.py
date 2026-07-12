"""Smoke test for interpreter-parallel prefetch (POC). Python >= 3.14, real GCS.

Two independent checks:
  1. Own-GIL import gate: create an own-GIL subinterpreter and import grpc +
     the async MRD inside it. If this raises "does not support loading in
     subinterpreters", the hypothesis is falsified at the dependency level.
  2. Fetch path: run worker_init + worker_fetch against a real object and verify
     shared memory is filled with the expected number of bytes.

Usage (run from the repo root or anywhere; ADC must be available):
    GCSFS_ZONAL_TEST_BUCKET=<your_bucket> \
    python3.14 gcsfs/tests/perf/interpreter_prefetch_smoke.py <object_name> <nbytes>
"""

import os
import sys

if sys.version_info < (3, 14):
    raise SystemExit("Requires CPython >= 3.14")

from multiprocessing import shared_memory

import google.auth
import google.auth.transport.requests
from concurrent.interpreters import create as create_interpreter # Python 3.14+


def _fresh_token() -> str:
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def check_own_gil_import() -> None:
    interp = create_interpreter() # own GIL by default on 3.14
    try:
        interp.exec(
            "import grpc\n"
            "from google.cloud.storage.asyncio.async_multi_range_downloader "
            "import AsyncMultiRangeDownloader\n"
            "print('[1] own-GIL import OK: grpc + AsyncMultiRangeDownloader')"
        )
    finally:
        interp.close()


def check_worker_fetch(bucket: str, object_name: str, nbytes: int) -> None:
    from gcsfs.interpreter_prefetch import _import_worker

    worker = _import_worker()
    token = _fresh_token()
    shm = shared_memory.SharedMemory(create=True, size=nbytes)
    try:
        worker.worker_init(bucket, object_name, None, token)
        written = worker.worker_fetch(0, nbytes, shm.name, 0)
        assert written == nbytes, f"expected {nbytes} bytes, wrote {written}"
        print(
            f"[2] worker_fetch OK: {written} bytes, head={bytes(shm.buf[:16])!r}"
        )
    finally:
        shm.close()
        shm.unlink()


def main() -> None:
    object_name = sys.argv[1]
    nbytes = int(sys.argv[2])
    bucket = os.environ["GCSFS_ZONAL_TEST_BUCKET"]

    check_own_gil_import()
    check_worker_fetch(bucket, object_name, nbytes)
    print("SMOKE OK")


if __name__ == "__main__":
    main()