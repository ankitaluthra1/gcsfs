"""
Benchmarks for cat_ranges method.
"""
import os
import pytest
import uuid
import random
from gcsfs.tests.settings import TEST_BUCKET, TEST_ZONAL_BUCKET

# Constants
FILE_SIZE = 5 * 1024 * 1024  # 5 MB
NUM_RANGES = 100
RANGE_SIZE = 1024  # 1 KB
NUM_FILES = 50

REQUIRED_ENV_VAR = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"
should_run_zonal = os.getenv(REQUIRED_ENV_VAR, "false").lower() in ("true", "1")


@pytest.fixture(scope="function")
def single_file(extended_gcsfs):
    """Creates a single large file in standard bucket."""
    fn = f"{TEST_BUCKET}/bench_cat_ranges_single_{uuid.uuid4()}"
    data = b"x" * FILE_SIZE
    extended_gcsfs.pipe(fn, data)
    yield fn
    try:
        extended_gcsfs.rm(fn)
    except Exception:
        pass


@pytest.fixture(scope="function")
def multiple_files(extended_gcsfs):
    """Creates multiple small files in standard bucket."""
    prefix = f"{TEST_BUCKET}/bench_cat_ranges_multi_{uuid.uuid4()}"
    files = [f"{prefix}/file_{i}" for i in range(NUM_FILES)]
    data = b"x" * RANGE_SIZE
    # Create files
    extended_gcsfs.pipe({fn: data for fn in files})
    yield files
    try:
        extended_gcsfs.rm(files)
    except Exception:
        pass


@pytest.fixture(scope="function")
def zonal_single_file(extended_gcsfs):
    """Creates a single large file in zonal bucket."""
    fn = f"{TEST_ZONAL_BUCKET}/bench_zonal_single_{uuid.uuid4()}"
    data = b"z" * FILE_SIZE
    extended_gcsfs.pipe(fn, data)
    yield fn
    try:
        extended_gcsfs.rm(fn)
    except Exception:
        pass


def test_cat_ranges_single_file_standard(extended_gcsfs, single_file, benchmark):
    """Benchmark cat_ranges on a single file in standard bucket."""
    paths = [single_file] * NUM_RANGES
    starts = [random.randint(0, FILE_SIZE - RANGE_SIZE) for _ in range(NUM_RANGES)]
    ends = [s + RANGE_SIZE for s in starts]

    def run_benchmark():
        extended_gcsfs.cat_ranges(paths, starts, ends)

    benchmark(run_benchmark)


def test_cat_ranges_multiple_files_standard(extended_gcsfs, multiple_files, benchmark):
    """Benchmark cat_ranges on multiple files in standard bucket."""
    paths = multiple_files
    starts = [0] * len(paths)
    ends = [RANGE_SIZE] * len(paths)

    def run_benchmark():
        extended_gcsfs.cat_ranges(paths, starts, ends)

    benchmark(run_benchmark)


@pytest.mark.skipif(
    not should_run_zonal,
    reason=f"Skipping tests: {REQUIRED_ENV_VAR} env variable is not set",
)
def test_cat_ranges_single_file_zonal(extended_gcsfs, zonal_single_file, benchmark):
    """Benchmark cat_ranges on a single file in zonal bucket."""
    paths = [zonal_single_file] * NUM_RANGES
    starts = [random.randint(0, FILE_SIZE - RANGE_SIZE) for _ in range(NUM_RANGES)]
    ends = [s + RANGE_SIZE for s in starts]

    def run_benchmark():
        extended_gcsfs.cat_ranges(paths, starts, ends)

    benchmark(run_benchmark)
