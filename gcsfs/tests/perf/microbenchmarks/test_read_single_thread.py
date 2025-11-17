import logging

from gcsfs.tests.perf.microbenchmarks.conftest import (
    BENCHMARK_ITERATIONS,
    BENCHMARK_ROUNDS,
    BUCKET_NAME,
    BENCHMARK_WARMUP_ROUNDS,
)


def test_list_objects(benchmark, gcs_read_benchmark_fixture):
    """Benchmark for listing objects within the test bucket."""
    gcs, _, num_files, file_size = gcs_read_benchmark_fixture
    logging.info(
        f"Running list_objects benchmark for {num_files} files of size {file_size // 1024 // 1024}MB."
    )
    benchmark.extra_info["num_files"] = num_files
    benchmark.extra_info["file_size"] = file_size
    benchmark.group = "LIST_OBJECTS"

    benchmark.pedantic(
        lambda: gcs.ls(BUCKET_NAME),
        iterations=BENCHMARK_ITERATIONS,
        rounds=BENCHMARK_ROUNDS,
        warmup_rounds=BENCHMARK_WARMUP_ROUNDS,
    )


def test_read_files(benchmark, gcs_read_benchmark_fixture):
    """Benchmark for reading a list of files one by one."""
    gcs, file_paths, num_files, file_size = gcs_read_benchmark_fixture
    logging.info(
        f"Running read_files benchmark for {num_files} files of size {file_size // 1024 // 1024}MB."
    )
    benchmark.extra_info["num_files"] = num_files
    benchmark.extra_info["file_size"] = file_size
    benchmark.extra_info["type"] = 'read'
    benchmark.group = "READ_OBJECTS"

    benchmark.pedantic(
        lambda: [gcs.cat(path) for path in file_paths],
        iterations=BENCHMARK_ITERATIONS,
        rounds=BENCHMARK_ROUNDS,
        warmup_rounds=BENCHMARK_WARMUP_ROUNDS,
    )