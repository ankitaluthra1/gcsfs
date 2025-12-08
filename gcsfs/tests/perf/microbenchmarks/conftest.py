import logging
import os
import uuid

# The order of these imports is critical.
# `import gcsfs` must come before `import fsspec` to ensure that the logic
# in `gcsfs/__init__.py` runs first. This allows gcsfs to check the
# GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT environment variable and patch the
# GCSFileSystem class before fsspec discovers and caches the implementation.
import gcsfs
import fsspec
import pytest
from gcsfs import GCSFileSystem


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Benchmark Configuration from Environment Variables ---
NUM_FILES = int(os.environ.get("GCSFS_BENCH_NUM_FILES", 1))
FILE_SIZE_MEGABYTES = int(os.environ.get("GCSFS_BENCH_FILE_SIZE_MB", 1))
FILE_SIZE_BYTES = FILE_SIZE_MEGABYTES * 1024 * 1024
CHUNK_SIZE_MB = int(os.environ.get("GCSFS_BENCH_CHUNK_SIZE_MB", 16))
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024
BENCHMARK_GROUP = os.environ.get("GCSFS_BENCH_GROUP", "read") # read, write, or list
BENCHMARK_PATTERN = os.environ.get("GCSFS_BENCH_PATTERN", "seq") # seq or rand
BENCHMARK_THREADS = int(os.environ.get("GCSFS_BENCH_THREADS", 1))

# Ensure chunk size is not larger than file size
CHUNK_SIZE_BYTES = min(CHUNK_SIZE_BYTES, FILE_SIZE_BYTES)

# --- Hierarchical Benchmark Configuration ---
DEPTH = int(os.environ.get("GCSFS_BENCH_DEPTH", 1))

BUCKET_NAME = os.environ.get("GCSFS_BENCH_BUCKET", "")
BUCKET_TYPE = os.environ.get("GCSFS_BENCH_BUCKET_TYPE", "regional")
GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT = os.environ.get("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false")

# --- Benchmark Execution Constants ---
BENCHMARK_ROUNDS = int(os.environ.get("GCSFS_BENCH_ROUNDS", 10))
BENCHMARK_WARMUP_ROUNDS = int(os.environ.get("GCSFS_BENCH_WARMUP", 1))

# Create combinations of (num_files, file_size)
benchmark_params = [(NUM_FILES, FILE_SIZE_BYTES)]

@pytest.fixture(scope="function")
def gcs_filesystem_factory():
    """Returns a factory that creates a new GCSFileSystem instance."""
    def factory(**kwargs):
        logging.info(f"Creating new GCSFileSystem instance for '{BUCKET_TYPE}', with EXPERIMENTAL_ZB_HNS_SUPPORT='{GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT}'.")
        GCSFileSystem.clear_instance_cache()
        return GCSFileSystem(**kwargs)
    return factory


@pytest.fixture(scope="function")
def gcs_benchmark_fixture(request, gcs_filesystem_factory):
    """
    A generic fixture that sets up and tears down the benchmarking environment.
    - For 'read' and 'write' benchmarks, it creates flat files with a specified size.
    - It handles Regional, HNS and zonal bucket setups.
    - For zonal buckets, we only add single object for now, this code will be removed once GCSFS has ZB write functionality
    """
    num_files = NUM_FILES
    file_size_bytes = FILE_SIZE_BYTES if BENCHMARK_GROUP in ['read', 'write'] else 0

    gcs = gcs_filesystem_factory()

    regional_file_prefix = os.environ.get("GCSFS_BENCH_REGIONAL_FILE_PATH")
    if regional_file_prefix:
        logging.info(
            "Regional benchmark with pre-created files detected. "
            "Skipping file creation."
        )
        base_path = regional_file_prefix.replace("gs://", "")
        file_paths = [f"{base_path}-{i}" for i in range(1, NUM_FILES + 1)]

        # Yield without creating files and skip teardown
        yield gcs, file_paths, b"", num_files, file_size_bytes
        return

    if BUCKET_TYPE == "zonal":
        logging.info("Zonal benchmark detected. Reusing pre-created file.")
        zonal_file_prefix = os.environ.get("GCSFS_BENCH_ZONAL_FILE_PATH")
        if not zonal_file_prefix:
            pytest.fail("GCSFS_BENCH_ZONAL_FILE_PATH env var not set for zonal benchmark.")

        # Reconstruct file paths based on the prefix and NUM_FILES
        base_path = zonal_file_prefix.replace("gs://", "")
        file_paths = [f"{base_path}-{i}" for i in range(1, NUM_FILES + 1)]

        yield gcs, file_paths, b"", num_files, file_size_bytes
        return

    logging.info(
        f"Setting up '{BENCHMARK_GROUP}' benchmark: {num_files} files, "
        f"{file_size_bytes // 1024 // 1024}MB each."
    )

    # Setup: Create files based on benchmark type
    file_paths = [f"{BUCKET_NAME}/{uuid.uuid4()}" for _ in range(num_files)]
    file_content = os.urandom(file_size_bytes)
    # For write benchmarks, we only need the paths and content, not to create files beforehand.
    if BENCHMARK_GROUP == "read":
        if file_size_bytes > 0:
            gcs.pipe({path: file_content for path in file_paths})
    
    yield gcs, file_paths, file_content, num_files, file_size_bytes

    # Teardown
    # For both 'read' and 'write', the files specified in `file_paths` need to be deleted.
    # - 'read' files are created during setup.
    # - 'write' files are created during the test itself.
    if BENCHMARK_GROUP in ["read", "write"]:
        logging.info(f"Tearing down benchmark. Deleting {len(file_paths)} files.")
        gcs.rm(file_paths)
        logging.info("Teardown complete.")