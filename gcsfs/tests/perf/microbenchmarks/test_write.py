import logging

from gcsfs.tests.perf.microbenchmarks.conftest import (
    BENCHMARK_ITERATIONS,
    BENCHMARK_ROUNDS,
    BUCKET_NAME,
    CHUNK_SIZE_BYTES,
    BUCKET_TYPE,
    BENCHMARK_WARMUP_ROUNDS,
)


def test_write(benchmark, gcs_benchmark_fixture):
    """Benchmark for writing a number of files sequentially."""
    gcs, file_paths, data, num_files, file_size = gcs_benchmark_fixture
    logging.info(
        f"Running write_files benchmark for {num_files} files of size {file_size // 1024 // 1024}MB with chunk size {CHUNK_SIZE_BYTES // 1024 // 1024}MB."
    )
    benchmark.extra_info["num_files"] = num_files
    benchmark.extra_info["bucket_name"] = BUCKET_NAME
    benchmark.extra_info["file_size"] = file_size
    benchmark.extra_info["chunk_size"] = CHUNK_SIZE_BYTES
    benchmark.extra_info["bucket_type"] = BUCKET_TYPE
    benchmark.group = "WRITE_OBJECTS"

    def write_op(paths, content, chunk):
        for path in paths:
            with gcs.open(path, "wb") as f:
                for i in range(0, len(content), chunk):
                    f.write(content[i:i + chunk])

    benchmark.pedantic(
        write_op,
        args=(file_paths, data, CHUNK_SIZE_BYTES),
        iterations=BENCHMARK_ITERATIONS,
        rounds=BENCHMARK_ROUNDS,
        warmup_rounds=BENCHMARK_WARMUP_ROUNDS,
    )