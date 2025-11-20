import logging

from gcsfs.tests.perf.microbenchmarks.conftest import (
    BENCHMARK_ITERATIONS,
    BENCHMARK_ROUNDS,
    BENCHMARK_WARMUP_ROUNDS,
    BUCKET_NAME,
    BUCKET_TYPE,
)


def test_list_and_verify_count(benchmark, gcs_hierarchical_benchmark_fixture):
    """Benchmark for listing objects within the test bucket."""
    gcs, base_dir, total_files = gcs_hierarchical_benchmark_fixture
    logging.info(
        f"Running list_objects benchmark for {total_files} files and verifying count."
    )
    benchmark.extra_info["num_files"] = total_files
    benchmark.extra_info["bucket_name"] = BUCKET_NAME
    benchmark.extra_info["file_size"] = 0
    benchmark.extra_info["bucket_type"] = BUCKET_TYPE
    benchmark.group = "LIST_OBJECTS"

    def list_and_verify():
        listed_files = gcs.ls(base_dir, detail=False, recursive=True)
        assert len(listed_files) == total_files

    benchmark.pedantic(
        list_and_verify,
        iterations=BENCHMARK_ITERATIONS,
        rounds=BENCHMARK_ROUNDS,
        warmup_rounds=BENCHMARK_WARMUP_ROUNDS,
    )