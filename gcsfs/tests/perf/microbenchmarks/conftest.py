import logging
import os
import uuid

import pytest

from gcsfs.tests.conftest import gcs_factory

def publish_benchmark_extra_info(benchmark, params, benchmark_group):
    """
    Helper function to publish benchmark parameters to the extra_info property.
    """
    benchmark.extra_info["num_files"] = params.num_files
    benchmark.extra_info["file_size"] = params.file_size_bytes
    benchmark.extra_info["chunk_size"] = params.chunk_size_bytes
    benchmark.extra_info["block_size"] = params.block_size_bytes
    benchmark.extra_info["pattern"] = params.pattern
    benchmark.extra_info["threads"] = params.num_threads
    benchmark.extra_info["rounds"] = params.rounds
    benchmark.extra_info["bucket_name"] = params.bucket_name
    benchmark.extra_info["bucket_type"] = params.bucket_type
    benchmark.group = benchmark_group


@pytest.fixture
def gcsfs_benchmark_read_write(gcs_factory, request):
    """
    A fixture that creates temporary files for a benchmark run and cleans
    them up afterward.

    It uses the `BenchmarkParameters` object from the test's parametrization
    to determine how many files to create and of what size.
    """
    params = request.param
    gcs = gcs_factory(block_size=params.block_size_bytes)
    if not gcs.exists(params.bucket_name):
        gcs.mkdir(params.bucket_name)

    prefix = f"{params.bucket_name}/benchmark-files-{uuid.uuid4()}"
    file_paths = [f"{prefix}/file_{i}" for i in range(params.num_files)]

    logging.info(
        f"Setting up benchmark '{params.name}': creating {params.num_files} file(s) "
        f"of size {params.file_size_bytes / 1024 / 1024:.2f} MB each."
    )

    # Define a 16MB chunk size for writing
    chunk_size = 16 * 1024 * 1024
    chunks_to_write = params.file_size_bytes // chunk_size
    remainder = params.file_size_bytes % chunk_size

    # Create files by writing random chunks to avoid high memory usage
    for path in file_paths:
        with gcs.open(path, "wb") as f:
            for _ in range(chunks_to_write):
                f.write(os.urandom(chunk_size))
            if remainder > 0:
                f.write(os.urandom(remainder))

    yield gcs, file_paths, params

    # --- Teardown ---
    logging.info(f"Tearing down benchmark '{params.name}': deleting files.")
    try:
        gcs.rm(prefix, recursive=True)
    except Exception as e:
        logging.error(f"Failed to clean up benchmark files: {e}")
