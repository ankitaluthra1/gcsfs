import logging

from gcsfs.tests.perf.microbenchmarks.conftest import (
    BENCHMARK_ITERATIONS,
    BENCHMARK_ROUNDS,
    BENCHMARK_WARMUP_ROUNDS,
    BUCKET_NAME,
)


def test_list(benchmark, gcs_list_benchmark_fixture):
    """Benchmark for listing objects within the test bucket."""
    gcs, _, num_files, file_size = gcs_list_benchmark_fixture
    logging.info(
        f"Running list_objects benchmark for {num_files} files."
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