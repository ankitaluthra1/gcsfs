import logging
import random
from concurrent.futures import ThreadPoolExecutor

from gcsfs.tests.perf.microbenchmarks.conftest import (
    BENCHMARK_ROUNDS,
    BUCKET_NAME,
    BENCHMARK_PATTERN,
    CHUNK_SIZE_BYTES,
    BUCKET_TYPE,
    BENCHMARK_THREADS,
    BENCHMARK_WARMUP_ROUNDS,
)

def _write_op_seq(gcs, paths, content, chunk):
    for path in paths:
        with gcs.open(path, "wb") as f:
            for i in range(0, len(content), chunk):
                f.write(content[i : i + chunk])


def _multi_thread_write_op_seq(gcs, paths, content, chunk):
    with ThreadPoolExecutor(max_workers=BENCHMARK_THREADS) as executor:
        # Each thread writes one full file sequentially
        list(executor.map(lambda path: _write_op_seq(gcs, [path], content, chunk), paths))


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
    benchmark.extra_info["pattern"] = BENCHMARK_PATTERN
    benchmark.extra_info["threads"] = BENCHMARK_THREADS
    benchmark.group = "WRITE_OBJECTS"

    # --- Main logic to select the operation ---
    if BENCHMARK_PATTERN == "rand":
        logging.warning("Random write pattern is not supported and will be skipped.")
        benchmark.skip("Random write pattern is not supported for gcsfs.")
        return

    if BENCHMARK_THREADS > 1:
        op = _multi_thread_write_op_seq
        op_args = (gcs, file_paths, data, CHUNK_SIZE_BYTES)
    else:  # Single-threaded
        op = _write_op_seq
        op_args = (gcs, file_paths, data, CHUNK_SIZE_BYTES)
    benchmark.pedantic(
        op,
        args=op_args,
        rounds=BENCHMARK_ROUNDS,
        warmup_rounds=BENCHMARK_WARMUP_ROUNDS,
    )