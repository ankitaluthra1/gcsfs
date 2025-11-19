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

# Ensure chunk size is not larger than file size
CHUNK_SIZE_BYTES = min(CHUNK_SIZE_BYTES, FILE_SIZE_BYTES)

BUCKET_NAME = os.environ.get("GCSFS_BENCH_BUCKET", "")
PROJECT_ID = os.environ.get("GCSFS_BENCH_PROJECT", "")

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


@pytest.fixture(scope="function", params=benchmark_params)
def gcs_read_benchmark_fixture(request):
    """
    A fixture that sets up and tears down the read benchmarking environment.
    It creates a parameterized number of files before the test and cleans them up after.
    """
    num_files, file_size = request.param
    logging.info(
        f"Setting up read benchmark: {num_files} files, {file_size // 1024 // 1024}MB each."
    )
    gcs = get_gcs_filesystem(PROJECT_ID)
    file_paths = [f"{BUCKET_NAME}/{uuid.uuid4()}" for _ in range(num_files)]
    file_content = os.urandom(file_size)

    # Define a fixed chunk size for efficient setup uploads.
    # For files smaller than this, the file size itself is used.
    SETUP_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB
    upload_chunk_size = min(file_size, SETUP_CHUNK_SIZE) if file_size > 0 else 0

    # Setup: Efficiently, create the files in chunks
    for path in file_paths:
        with gcs.open(path, "wb") as f:
            for i in range(0, file_size, upload_chunk_size or 1):
                f.write(file_content[i:i + upload_chunk_size])
    logging.info(f"Setup complete. Created {num_files} files.")

    yield gcs, file_paths, num_files, file_size

    # Teardown: Delete the files
    logging.info(f"Tearing down benchmark. Deleting {num_files} files.")
    gcs.rm(file_paths)
    logging.info("Teardown complete.")

@pytest.fixture(scope="function", params=benchmark_params)
def gcs_write_benchmark_fixture(request):
    """
    A fixture that prepares for and cleans up after a write benchmark.
    It generates file paths and content, and deletes the created files after the test.
    """
    num_files, file_size = request.param
    logging.info(
        f"Setting up write benchmark: {num_files} files, {file_size // 1024 // 1024}MB each."
    )
    gcs = get_gcs_filesystem(PROJECT_ID)
    file_paths = [f"{BUCKET_NAME}/{uuid.uuid4()}" for _ in range(num_files)]
    file_content = os.urandom(file_size)

    yield gcs, file_paths, file_content, num_files, file_size

    # Teardown: Delete the files
    logging.info(f"Tearing down benchmark. Deleting {num_files} files.")
    gcs.rm(file_paths)
    logging.info("Teardown complete.")