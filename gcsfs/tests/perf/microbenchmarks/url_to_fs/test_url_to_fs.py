import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
from fsspec.core import url_to_fs

from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_multi_threaded,
    run_single_threaded,
)
from gcsfs.tests.perf.microbenchmarks.url_to_fs.configs import (
    get_url_to_fs_benchmark_cases,
)

BENCHMARK_GROUP = "url_to_fs"


def _url_to_fs_op(path):
    start_time = time.perf_counter()
    url_to_fs(f"gs://{path}")
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.debug(f"URL_TO_FS : gs://{path} - {duration_ms:.2f} ms.")


def _url_to_fs_ops(paths):
    for path in paths:
        _url_to_fs_op(path)


all_benchmark_cases = get_url_to_fs_benchmark_cases()
single_threaded_cases, multi_threaded_cases, multi_process_cases = filter_test_cases(
    all_benchmark_cases
)


@pytest.mark.parametrize(
    "gcsfs_benchmark_url_to_fs",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_url_to_fs_single_threaded(benchmark, gcsfs_benchmark_url_to_fs, monitor):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_url_to_fs

    run_single_threaded(
        benchmark, monitor, params, _url_to_fs_ops, (file_paths,), BENCHMARK_GROUP
    )


def _chunk_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


@pytest.mark.parametrize(
    "gcsfs_benchmark_url_to_fs",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_url_to_fs_multi_threaded(benchmark, gcsfs_benchmark_url_to_fs, monitor):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_url_to_fs

    chunks = _chunk_list(file_paths, params.threads)
    args_list = [(chunks[i],) for i in range(params.threads)]

    run_multi_threaded(
        benchmark, monitor, params, _url_to_fs_ops, args_list, BENCHMARK_GROUP
    )


def _process_worker(paths, threads, process_durations_shared, index):
    """A worker function for each process to run url_to_fs operations."""
    start_time = time.perf_counter()
    chunks = _chunk_list(paths, threads)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(_url_to_fs_ops, chunks[i]) for i in range(threads)]
        for f in futures:
            f.result()
    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


@pytest.mark.parametrize(
    "gcsfs_benchmark_url_to_fs",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_url_to_fs_multi_process(
    benchmark, gcsfs_benchmark_url_to_fs, extended_gcs_factory, request, monitor
):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_url_to_fs

    chunks = _chunk_list(file_paths, params.processes)

    def args_builder(gcs_instance, i, shared_arr):
        return (
            chunks[i],
            params.threads,
            shared_arr,
            i,
        )

    run_multi_process(
        benchmark,
        monitor,
        params,
        extended_gcs_factory,
        worker_target=_process_worker,
        args_builder=args_builder,
        benchmark_group=BENCHMARK_GROUP,
        request=request,
    )
