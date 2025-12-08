import logging
import multiprocessing
import random
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.conftest import publish_benchmark_extra_info
from gcsfs.tests.perf.microbenchmarks.read.configs import get_read_benchmark_cases

BENCHMARK_GROUP = "read"


def _read_op_seq(gcs, path, chunk_size):
    start_time = time.perf_counter()
    with gcs.open(path, "rb") as f:
        while f.read(chunk_size):
            pass
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"SEQ_READ : {path} - {duration_ms:.2f} ms.")


def _read_op_rand(gcs, path, chunk_size, offsets):
    start_time = time.perf_counter()
    with gcs.open(path, "rb") as f:
        for offset in offsets:
            f.seek(offset)
            f.read(chunk_size)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"RAND_READ : {path} - {duration_ms:.2f} ms.")


all_benchmark_cases = get_read_benchmark_cases()

single_threaded_cases = [
    p for p in all_benchmark_cases if p.num_threads == 1 and p.num_processes == 1
]
multi_threaded_cases = [
    p for p in all_benchmark_cases if p.num_threads > 1 and p.num_processes == 1
]
multi_process_cases = [p for p in all_benchmark_cases if p.num_processes > 1]


@pytest.mark.parametrize(
    "gcsfs_benchmark_read_write",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_single_threaded(benchmark, gcsfs_benchmark_read_write):
    gcs, file_paths, params = gcsfs_benchmark_read_write

    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)
    path = file_paths[0]

    if params.pattern == "seq":
        op = _read_op_seq
        op_args = (gcs, path, params.chunk_size_bytes)
        benchmark.pedantic(op, args=op_args, rounds=params.rounds)
    elif params.pattern == "rand":
        offsets = list(range(0, params.file_size_bytes, params.chunk_size_bytes))
        op = _read_op_rand
        op_args = (gcs, path, params.chunk_size_bytes, offsets)
        benchmark.pedantic(op, args=op_args, rounds=params.rounds)


@pytest.mark.parametrize(
    "gcsfs_benchmark_read_write",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_multi_threaded(benchmark, gcsfs_benchmark_read_write):
    gcs, file_paths, params = gcsfs_benchmark_read_write

    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    def run_benchmark():
        logging.info("Multi-threaded benchmark: Starting benchmark round.")
        with ThreadPoolExecutor(max_workers=params.num_threads) as executor:
            if params.pattern == "seq":
                # Each thread reads one full file sequentially.
                futures = [
                    executor.submit(_read_op_seq, gcs, path, params.chunk_size_bytes)
                    for path in file_paths
                ]
                list(futures)  # Wait for completion

            elif params.pattern == "rand":

                def random_read_worker(path):
                    # Each worker gets its own shuffled list of offsets.
                    local_offsets = list(offsets)
                    random.shuffle(local_offsets)
                    _read_op_rand(gcs, path, params.chunk_size_bytes, local_offsets)

                offsets = list(
                    range(0, params.file_size_bytes, params.chunk_size_bytes)
                )

                if params.num_files == 1:
                    # All threads read the same file randomly.
                    paths_to_read = [file_paths[0]] * params.num_threads
                else:
                    # Each thread reads a different file randomly.
                    paths_to_read = file_paths

                futures = [
                    executor.submit(random_read_worker, path) for path in paths_to_read
                ]
                list(futures)  # Wait for completion

    benchmark.pedantic(run_benchmark, rounds=params.rounds)


def _process_worker(gcs, file_paths, chunk_size, num_threads, pattern, file_size_bytes):
    """A worker function for each process to read a list of files."""
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        if pattern == "seq":
            futures = [
                executor.submit(_read_op_seq, gcs, path, chunk_size)
                for path in file_paths
            ]
        elif pattern == "rand":
            offsets = list(range(0, file_size_bytes, chunk_size))

            def random_read_worker(path):
                # Each worker gets its own shuffled list of offsets.
                local_offsets = list(offsets)
                random.shuffle(local_offsets)
                _read_op_rand(gcs, path, chunk_size, local_offsets)

            futures = [executor.submit(random_read_worker, path) for path in file_paths]

            # Wait for all threads in the process to complete
            list(futures)


@pytest.mark.parametrize(
    "gcsfs_benchmark_read_write",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_multi_process(benchmark, gcsfs_benchmark_read_write):
    gcs, file_paths, params = gcsfs_benchmark_read_write
    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    # Set the start method to 'spawn' to avoid deadlocks in the child process
    # when the parent process is multi-threaded. This is a common issue when
    # running multiprocessing tests with pytest.
    if multiprocessing.get_start_method(allow_none=True) != "spawn":
        multiprocessing.set_start_method("spawn", force=True)

    # Manual benchmark loop for multi-process tests
    timings_s = []
    for _ in range(params.rounds):
        logging.info("Multi-process benchmark: Starting benchmark round.")
        processes = []
        files_per_process = params.num_files // params.num_processes
        threads_per_process = params.num_threads

        for i in range(params.num_processes):
            if params.num_files > 1:
                start_index = i * files_per_process
                end_index = start_index + files_per_process
                process_files = file_paths[start_index:end_index]
            else:  # num_files == 1
                # Each process will have its threads read from the same single file
                process_files = [file_paths[0]] * threads_per_process

            p = multiprocessing.Process(
                target=_process_worker,
                args=(
                    gcs,
                    process_files,
                    params.chunk_size_bytes,
                    threads_per_process,
                    params.pattern,
                    params.file_size_bytes,
                ),
            )
            processes.append(p)
            p.start()

        start_time = time.perf_counter()
        for p in processes:
            p.join()
        duration_s = time.perf_counter() - start_time
        timings_s.append(duration_s)

    # Calculate and print statistics
    min_time = min(timings_s)
    max_time = max(timings_s)
    mean_time = sum(timings_s) / len(timings_s)

    # Build the results table as a single multi-line string to log it cleanly.
    results_table = (
        f"\n{'-' * 90}\n"
        f"{'Name (time in s)':<50s} {'Min':>8s} {'Max':>8s} {'Mean':>8s} {'Rounds':>8s}\n"
        f"{'-' * 90}\n"
        f"{params.name:<50s} {min_time:>8.4f} {max_time:>8.4f} {mean_time:>8.4f} {params.rounds:>8d}\n"
        f"{'-' * 90}"
    )
    logging.info(f"Multi-process benchmark results:{results_table}")
