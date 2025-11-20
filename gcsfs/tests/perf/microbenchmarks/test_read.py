import logging

from gcsfs.tests.perf.microbenchmarks.conftest import (
    BENCHMARK_ITERATIONS,
    BENCHMARK_ROUNDS,
    BENCHMARK_WARMUP_ROUNDS,
    BUCKET_NAME,
    BUCKET_TYPE,
    CHUNK_SIZE_BYTES,
)
 
def test_read(benchmark, gcs_benchmark_fixture):
    """Benchmark for reading a list of files one by one."""
    gcs, file_paths, _, num_files, file_size = gcs_benchmark_fixture
    logging.info(
        f"Running read_files benchmark for {num_files} files of size {file_size // 1024 // 1024}MB with chunk size {CHUNK_SIZE_BYTES // 1024 // 1024}MB."
    )
    benchmark.extra_info["num_files"] = num_files
    benchmark.extra_info["bucket_name"] = BUCKET_NAME
    benchmark.extra_info["file_size"] = file_size
    benchmark.extra_info["chunk_size"] = CHUNK_SIZE_BYTES
    benchmark.extra_info["bucket_type"] = BUCKET_TYPE
    benchmark.group = "READ_OBJECTS"

    def read_op(paths, chunk):
        for path in paths:
            with gcs.open(path, "rb") as f:
                f.seek(0)  # Ensure read starts from the beginning
                while f.read(chunk):
                    pass

    benchmark.pedantic(
        read_op,
        args=(file_paths, CHUNK_SIZE_BYTES),
        iterations=BENCHMARK_ITERATIONS,
        rounds=BENCHMARK_ROUNDS,
        warmup_rounds=BENCHMARK_WARMUP_ROUNDS,
    )