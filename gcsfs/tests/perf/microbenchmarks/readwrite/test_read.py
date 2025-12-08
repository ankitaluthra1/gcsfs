import random
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.tests.perf.microbenchmarks.readwrite.benchmark_configs import (
    get_read_benchmark_cases,
)

from gcsfs.tests.perf.microbenchmarks.conftest import publish_benchmark_extra_info

BENCHMARK_GROUP = "read"


def _read_op_seq(gcs, path, chunk_size):
    with gcs.open(path, "rb") as f:
        while f.read(chunk_size):
            pass


def _read_op_rand(gcs, path, chunk_size, offsets):
    with gcs.open(path, "rb") as f:
        for offset in offsets:
            f.seek(offset)
            f.read(chunk_size)


benchmark_cases = get_read_benchmark_cases()

@pytest.mark.parametrize(
    "gcsfs_benchmark_read_write", benchmark_cases, indirect=True, ids=lambda p: p.name
)
def test_read_single_threaded(benchmark, gcsfs_benchmark_read_write):
    gcs, file_paths, params = gcsfs_benchmark_read_write
    if params.num_threads != 1:
        pytest.skip("This test is for single-threaded reads.")

    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)
    path = file_paths[0]

    if params.pattern == "seq":
        benchmark.pedantic(
            _read_op_seq, gcs, path, params.chunk_size_bytes, rounds=params.rounds
        )
    elif params.pattern == "rand":
        offsets = list(range(0, params.file_size_bytes, params.chunk_size_bytes))
        random.shuffle(offsets)
        benchmark.pedantic(
            _read_op_rand, gcs, path, params.chunk_size_bytes, offsets, rounds=params.rounds
        )


@pytest.mark.parametrize(
    "gcsfs_benchmark_read_write", benchmark_cases, indirect=True, ids=lambda p: p.name
)
def test_read_multi_threaded(benchmark, gcsfs_benchmark_read_write):
    gcs, file_paths, params = gcsfs_benchmark_read_write
    if params.num_threads <= 1:
        pytest.skip("This test is for multi-threaded reads.")

    publish_benchmark_extra_info(benchmark, params, BENCHMARK_GROUP)

    def run_benchmark():
        with ThreadPoolExecutor(max_workers=params.num_threads) as executor:
            if params.pattern == "seq":
                # Each thread reads one full file sequentially.
                futures = [
                    executor.submit(_read_op_seq, gcs, path, params.chunk_size_bytes)
                    for path in file_paths
                ]
                list(futures) # Wait for completion

            elif params.pattern == "rand":

                def random_read_worker(path):
                    # Each worker gets its own shuffled list of offsets.
                    local_offsets = list(offsets)
                    random.shuffle(local_offsets)
                    _read_op_rand(gcs, path, params.chunk_size_bytes, local_offsets)

                offsets = list(range(0, params.file_size_bytes, params.chunk_size_bytes))

                if params.num_files == 1:
                    # All threads read the same file randomly.
                    paths_to_read = [file_paths[0]] * params.num_threads
                else:
                    # Each thread reads a different file randomly.
                    paths_to_read = file_paths

                futures = [executor.submit(random_read_worker, path) for path in paths_to_read]
                list(futures) # Wait for completion

    benchmark.pedantic(run_benchmark, rounds=params.rounds)
