import argparse
import itertools
import logging
import os
import uuid

import fsspec
import pytest

from gcsfs.tests.settings import TEST_BUCKET, TEST_PROJECT

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Benchmark Parameters ---
NUM_FILES = [10]  # [10, 100, 1000]
FILE_SIZES_BYTES = [1024]  # [1024, 1024 * 1024, 10 * 1024 * 1024]  # 1KB, 1MB, 10MB

# Create combinations of (num_files, file_size)
benchmark_params = list(itertools.product(NUM_FILES, FILE_SIZES_BYTES))


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
        f"Setting up read benchmark: {num_files} files, {file_size // 1024}KB each."
    )
    gcs = get_gcs_filesystem(TEST_PROJECT)
    file_paths = [f"{TEST_BUCKET}/{uuid.uuid4()}" for _ in range(num_files)]
    file_content = os.urandom(file_size)

    # Setup: Create the files
    for path in file_paths:
        with gcs.open(path, "wb") as f:
            f.write(file_content)
    logging.info(f"Setup complete. Created {num_files} files.")

    yield gcs, file_paths, num_files, file_size

    # Teardown: Delete the files
    logging.info(f"Tearing down benchmark. Deleting {num_files} files.")
    gcs.rm(file_paths)
    logging.info("Teardown complete.")


def get_benchmark_parser(description):
    """Creates a common argparse.ArgumentParser for benchmarks."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--project",
        type=str,
        default=TEST_PROJECT,
        help="The GCS project ID.",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=TEST_BUCKET,
        help="The GCS bucket name.",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=10,
        help="The number of files to create for the benchmark.",
    )
    parser.add_argument(
        "--file-size-kb",
        type=int,
        default=1,
        help="The size of each file in kilobytes (KB).",
    )
    return parser


def print_benchmark_summary(benchmark_results):
    """Prints a formatted summary table for benchmark results."""
    if not benchmark_results:
        return
    print("\n--- Benchmark Summary ---")
    headers = benchmark_results[0].keys()
    widths = {
        header: max(len(str(row[header])) for row in benchmark_results)
        for header in headers
    }
    widths = {h: max(len(h), w) for h, w in widths.items()}
    header_line = " | ".join(h.ljust(widths[h]) for h in headers)
    print(header_line)
    print("-" * len(header_line))
    for row in benchmark_results:
        row_line = " | ".join(str(row[h]).ljust(widths[h]) for h in headers)
        print(row_line)


@pytest.fixture(scope="function", params=benchmark_params)
def gcs_write_benchmark_fixture(request):
    """
    A fixture that prepares for and cleans up after a write benchmark.
    It generates file paths and content, and deletes the created files after the test.
    """
    num_files, file_size = request.param
    logging.info(
        f"Setting up write benchmark: {num_files} files, {file_size // 1024}KB each."
    )
    gcs = get_gcs_filesystem(TEST_PROJECT)
    file_paths = [f"{TEST_BUCKET}/{uuid.uuid4()}" for _ in range(num_files)]
    file_content = os.urandom(file_size)

    yield gcs, file_paths, file_content, num_files, file_size

    # Teardown: Delete the files
    logging.info(f"Tearing down benchmark. Deleting {num_files} files.")
    gcs.rm(file_paths)
    logging.info("Teardown complete.")