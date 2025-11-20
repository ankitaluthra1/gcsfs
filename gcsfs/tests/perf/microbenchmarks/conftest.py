import logging
import os
import uuid

import fsspec
import pytest

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

# Ensure chunk size is not larger than file size
CHUNK_SIZE_BYTES = min(CHUNK_SIZE_BYTES, FILE_SIZE_BYTES)

# --- Hierarchical Benchmark Configuration ---
DEPTH = int(os.environ.get("GCSFS_BENCH_DEPTH", 1))
FILES_PER_DIR = int(os.environ.get("GCSFS_BENCH_FILES_PER_DIR", 1))

BUCKET_NAME = os.environ.get("GCSFS_BENCH_BUCKET", "")
PROJECT_ID = os.environ.get("GCSFS_BENCH_PROJECT", "")
BUCKET_TYPE = os.environ.get("GCSFS_BENCH_BUCKET_TYPE", "regional")

# --- Benchmark Execution Constants ---
BENCHMARK_ROUNDS = int(os.environ.get("GCSFS_BENCH_ROUNDS", 10))
BENCHMARK_ITERATIONS = int(os.environ.get("GCSFS_BENCH_ITERATIONS", 1))
BENCHMARK_WARMUP_ROUNDS = int(os.environ.get("GCSFS_BENCH_WARMUP", 1))

# Create combinations of (num_files, file_size)
benchmark_params = [(NUM_FILES, FILE_SIZE_BYTES)]


def get_gcs_filesystem(project):
    """Creates and returns a GCSFileSystem instance."""
    # In a CI environment, we use the service account associated with the VM.
    # For local debugging, we use the user's default gcloud credentials.
    if os.environ.get("CI"):
        # The 'project' argument is used to select the correct project when
        # service account credentials are available.
        return fsspec.filesystem("gcs", project=project)

    # For local runs, 'google_default' uses credentials from
    # `gcloud auth application-default login`.
    return fsspec.filesystem("gcs", token="google_default")



@pytest.fixture(scope="function")
def gcs_benchmark_fixture(request):
    """
    A generic fixture that sets up and tears down the benchmarking environment.
    - For 'read' and 'write' benchmarks, it creates flat files with a specified size.
    - It handles Regional, HNS and zonal bucket setups.
    - For zonal buckets, we only add single object for now, this code will be removed once GCSFS has ZB write functionality
    """
    num_files = NUM_FILES
    file_size = FILE_SIZE_BYTES if BENCHMARK_GROUP in ["read", "write"] else 0

    if BUCKET_TYPE == "zonal":
        logging.info("Zonal benchmark detected. Reusing pre-created file.")
        zonal_file_path = os.environ.get("GCSFS_BENCH_ZONAL_FILE_PATH")
        if not zonal_file_path:
            pytest.fail("GCSFS_BENCH_ZONAL_FILE_PATH env var not set for zonal benchmark.")
        gcs = get_gcs_filesystem(PROJECT_ID)
        yield gcs, [zonal_file_path.replace("gs://", "")], 1, file_size
        return

    logging.info(
        f"Setting up '{BENCHMARK_GROUP}' benchmark: {num_files} files, "
        f"{file_size // 1024 // 1024}MB each."
    )
    gcs = get_gcs_filesystem(PROJECT_ID)

    # Setup: Create files based on benchmark type
    file_paths = [f"{BUCKET_NAME}/{uuid.uuid4()}" for _ in range(num_files)]
    file_content = os.urandom(file_size)
    # For write benchmarks, we only need the paths and content, not to create files beforehand.
    if BENCHMARK_GROUP == "read":
        SETUP_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB
        upload_chunk_size = min(file_size, SETUP_CHUNK_SIZE) if file_size > 0 else 0
        for path in file_paths:
            with gcs.open(path, "wb") as f:
                for i in range(0, file_size, upload_chunk_size or 1):
                    f.write(file_content[i:i + upload_chunk_size])
    
    yield gcs, file_paths, file_content, num_files, file_size

    # Teardown
    # For both 'read' and 'write', the files specified in `file_paths` need to be deleted.
    # - 'read' files are created during setup.
    # - 'write' files are created during the test itself.
    if BENCHMARK_GROUP in ["read", "write"]:
        logging.info(f"Tearing down benchmark. Deleting {len(file_paths)} files.")
        gcs.rm(file_paths)
        logging.info("Teardown complete.")


@pytest.fixture(scope="function")
def gcs_hierarchical_benchmark_fixture(request):
    """
    A fixture for hierarchical benchmarks (list, rename, delete).
    It creates a nested directory structure of zero-byte files.
    """
    gcs = get_gcs_filesystem(PROJECT_ID)
    base_dir = f"{BUCKET_NAME}/{uuid.uuid4()}"
    all_paths = []

    def create_files(current_path, current_depth):
        if current_depth > DEPTH:
            return
        # Create files at the current level
        for i in range(FILES_PER_DIR):
            file_path = f"{current_path}/file_{i}.txt"
            gcs.touch(file_path)
            all_paths.append(file_path)
        # Recurse to create a deeper directory
        create_files(f"{current_path}/subdir_{current_depth}", current_depth + 1)

    logging.info(f"Setting up hierarchical benchmark: depth={DEPTH}, files_per_dir={FILES_PER_DIR}")
    create_files(base_dir, 1)
    logging.info(f"Setup complete. Created {len(all_paths)} total files.")

    yield gcs, base_dir, len(all_paths)

    # Teardown
    logging.info(f"Tearing down benchmark. Deleting directory {base_dir}.")
    gcs.rm(base_dir, recursive=True)
    logging.info("Teardown complete.")