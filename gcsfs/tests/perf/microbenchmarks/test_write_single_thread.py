import logging

from gcsfs.tests.perf.microbenchmarks.conftest import (
    BENCHMARK_ITERATIONS,
    BENCHMARK_ROUNDS,
    BENCHMARK_WARMUP_ROUNDS,
)


def test_write_files(benchmark, gcs_write_benchmark_fixture):
    """Benchmark for writing a number of files sequentially."""
    gcs, file_paths, data, num_files, file_size = gcs_write_benchmark_fixture
    logging.info(
        f"Running write_files benchmark for {num_files} files of size {file_size // 1024 * 1024}MB."
    )
    benchmark.extra_info["num_files"] = num_files
    benchmark.extra_info["file_size"] = file_size
    benchmark.group = "WRITE_OBJECTS"

    def write_op(paths, content):
        for path in paths:
            with gcs.open(path, "wb") as f:
                f.write(content)

    benchmark.pedantic(
        write_op,
        args=(file_paths, data),
        iterations=BENCHMARK_ITERATIONS,
        rounds=BENCHMARK_ROUNDS,
        warmup_rounds=BENCHMARK_WARMUP_ROUNDS,
    )