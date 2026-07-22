"""Read a GCS object through the prefetcher and report time + throughput.

Whether the prefetcher uses own-GIL subinterpreters (instead of the default
asyncio concurrency) is controlled entirely by the USE_INTERPRETER_PARALLEL_PREFETCH
environment variable. When that variable is enabled, CPython >= 3.14 is required.

Usage:
    # asyncio path (default)
    python3.14 gcsfs/tests/perf/interpreter_prefetch_read.py <bucket/object> [concurrency]

    # own-GIL subinterpreter path
    USE_INTERPRETER_PARALLEL_PREFETCH=true \
    python3.14 gcsfs/tests/perf/interpreter_prefetch_read.py <bucket/object> [concurrency]
"""

import logging
import sys
import time

from gcsfs.interpreter_prefetch import interpreter_prefetch_enabled
from gcsfs import GCSFileSystem

_CHUNK = 16 * 1024 * 1024


def main():
  logging.basicConfig(level=logging.INFO)
  if len(sys.argv) < 2:
    raise SystemExit(
        "usage: interpreter_prefetch_read.py <bucket/object> [concurrency]"
    )

  path = sys.argv[1].removeprefix("gs://")
  concurrency = int(sys.argv[2]) if len(sys.argv) > 2 else 4
  mode = "interpreter-parallel" if interpreter_prefetch_enabled() else "asyncio"

  fs = GCSFileSystem()

  nbytes = 0
  t0 = time.perf_counter()
  with fs.open(
      path,
      mode="rb",
      use_experimental_adaptive_prefetching=True,
      concurrency=concurrency,
  ) as f:
    while True:
      chunk = f.read(_CHUNK)
      if not chunk:
        break
      nbytes += len(chunk)
  elapsed = time.perf_counter() - t0

  mb = nbytes / 1e6
  print(f"path={path}")
  print(f"mode={mode} concurrency={concurrency}")
  print(
    f"size={mb:.1f} MB time={elapsed:.2f} s throughput={mb / elapsed:.1f} MB/s")


if __name__ == "__main__":
  main()
