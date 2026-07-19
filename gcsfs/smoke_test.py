"""Smoke test for interpreter-parallel HTTP prefetch (standard buckets).
Python >= 3.14. Runs against a real standard bucket (or an HTTP emulator if
STORAGE_EMULATOR_HOST is set).

1. Own-GIL import gate: import the stdlib HTTP stack (ssl / socket /
   http.client / urllib) inside an own-GIL subinterpreter.
2. Fetch path: worker_init + worker_fetch download a real byte range into
   shared memory and verify the byte count.

Usage:
    GCSFS_TEST_BUCKET=<standard-bucket> \
    python3.14 gcsfs/tests/perf/interpreter_prefetch_smoke.py <object_name> <nbytes>
(ADC is used for the bearer token unless STORAGE_EMULATOR_HOST is set.)
"""

import os
import sys
from multiprocessing import shared_memory

from concurrent.interpreters import create as create_interpreter  # Python 3.14+


def _maybe_token():
  if os.environ.get("STORAGE_EMULATOR_HOST"):
    return None
  import google.auth
  import google.auth.transport.requests

  creds, _ = google.auth.default(
      scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
  )
  creds.refresh(google.auth.transport.requests.Request())
  return creds.token


def check_own_gil_import():
  interp = create_interpreter()  # own GIL by default on 3.14
  try:
    interp.exec(
        "import ssl, socket, http.client, urllib.parse\n"
        "ssl.create_default_context()\n"
        "print('[1] own-GIL stdlib HTTP import OK')"
    )
  finally:
    interp.close()


def check_fetch(bucket, object_name, nbytes):
  from gcsfs.interpreter_prefetch import _http_endpoint, _import_worker

  worker = _import_worker()
  host, scheme = _http_endpoint()
  worker.worker_init(bucket, object_name, None, _maybe_token(), host, scheme)
  shm = shared_memory.SharedMemory(create=True, size=nbytes)
  try:
    written = worker.worker_fetch(0, nbytes, shm.name, 0)
    assert written == nbytes, f"expected {nbytes} bytes, wrote {written}"
    print(
        f"[2] worker_fetch OK: {written} bytes from {scheme}://{host}"
        f"/{bucket}/{object_name} head={bytes(shm.buf[:16])!r}"
    )
  finally:
    shm.close()
    shm.unlink()


def main():
  if sys.version_info < (3, 14):
    raise SystemExit("Requires CPython >= 3.14")

  object_name = sys.argv[1]
  nbytes = int(sys.argv[2])
  bucket = os.environ["GCSFS_TEST_BUCKET"]

  check_own_gil_import()
  check_fetch(bucket, object_name, nbytes)
  print("SMOKE OK")


if __name__ == "__main__":
  main()
