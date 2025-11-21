import logging
import random
from concurrent.futures import ThreadPoolExecutor

from gcsfs.tests.perf.microbenchmarks.conftest import (
    BENCHMARK_ROUNDS,
    BENCHMARK_WARMUP_ROUNDS,
    BUCKET_NAME,
    BUCKET_TYPE,
    BENCHMARK_PATTERN,
    BENCHMARK_THREADS,
    CHUNK_SIZE_BYTES,
)

def _read_op_seq(gcs, paths, chunk):
    for path in paths:
        with gcs.open(path, "rb") as f:
            while f.read(chunk):
                pass

def _read_op_rand(gcs, paths, chunk, offsets):
    for path in paths:
        with gcs.open(path, "rb") as f:
            # Read the same number of chunks as are in the file, but from random offsets
            for _ in range(len(offsets)):
                f.seek(random.choice(offsets))
                f.read(chunk)

def _multi_thread_read_op_seq(gcs, paths, chunk):
    with ThreadPoolExecutor(max_workers=BENCHMARK_THREADS) as executor:
        # Each thread reads one full file sequentially
        list(executor.map(lambda path: _read_op_seq(gcs, [path], chunk), paths))


def _multi_thread_read_op_rand(gcs, path, chunk, offsets):
    if BENCHMARK_THREADS == 0:
        return
    num_reads_per_thread = len(offsets) // BENCHMARK_THREADS

    def worker():
        # A gcsfs instance is created per-thread by default, which is what we want
        # to ensure thread safety.
        with gcs.open(path, "rb") as f:
            for _ in range(num_reads_per_thread):
                f.seek(random.choice(offsets))
                f.read(chunk)

    with ThreadPoolExecutor(max_workers=BENCHMARK_THREADS) as executor:
        futures = [executor.submit(worker) for _ in range(BENCHMARK_THREADS)]
        [f.result() for f in futures]


def test_read(benchmark, gcs_benchmark_fixture):
    """Benchmark for reading files with single or multiple threads."""
    gcs, file_paths, _, num_files, file_size_bytes = gcs_benchmark_fixture
    logging.info(
        f"Running read_files benchmark for {num_files} files of size {file_size_bytes // 1024 // 1024}MB with chunk size {CHUNK_SIZE_BYTES // 1024 // 1024}MB."
    )
    benchmark.extra_info["num_files"] = num_files
    benchmark.extra_info["bucket_name"] = BUCKET_NAME
    benchmark.extra_info["file_size"] = file_size_bytes
    benchmark.extra_info["chunk_size"] = CHUNK_SIZE_BYTES
    benchmark.extra_info["pattern"] = BENCHMARK_PATTERN
    benchmark.extra_info["threads"] = BENCHMARK_THREADS
    benchmark.extra_info["bucket_type"] = BUCKET_TYPE
    benchmark.group = "READ_OBJECTS"

    # --- Main logic to select the operation ---
    if BENCHMARK_THREADS > 1:
        if BENCHMARK_PATTERN == "rand":
            if CHUNK_SIZE_BYTES == 0 or file_size_bytes < CHUNK_SIZE_BYTES: offsets = [0]
            else: offsets = list(range(0, file_size_bytes - CHUNK_SIZE_BYTES, CHUNK_SIZE_BYTES))
            op = _multi_thread_read_op_rand
            # For multi-threaded random, we operate on a single file
            op_args = (gcs, file_paths[0], CHUNK_SIZE_BYTES, offsets)
        else: # seq
            op = _multi_thread_read_op_seq
            op_args = (gcs, file_paths, CHUNK_SIZE_BYTES)
    else: # Single-threaded
        if BENCHMARK_PATTERN == "rand":
            if CHUNK_SIZE_BYTES == 0 or file_size_bytes < CHUNK_SIZE_BYTES: offsets = [0]
            else: offsets = list(range(0, file_size_bytes - CHUNK_SIZE_BYTES, CHUNK_SIZE_BYTES))
            op = _read_op_rand
            op_args = (gcs, file_paths, CHUNK_SIZE_BYTES, offsets)
        else: # seq
            op = _read_op_seq
            op_args = (gcs, file_paths, CHUNK_SIZE_BYTES)

    benchmark.pedantic(
        op,
        args=op_args,
        rounds=BENCHMARK_ROUNDS,
        warmup_rounds=BENCHMARK_WARMUP_ROUNDS,
    )