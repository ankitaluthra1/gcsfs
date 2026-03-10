import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from gcsfs.latency_tracker import latency_measurements
from gcsfs.tests.perf.microbenchmarks.info.configs import get_info_benchmark_cases
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_multi_process,
    run_multi_threaded,
    run_single_threaded,
)


def _print_latency_table():
    if not latency_measurements:
        return
    import collections

    stats = collections.defaultdict(list)
    output = []
    output.append("\n" + "=" * 80)
    output.append(f"{'Operation':<25} | {'Path':<35} | {'Latency (ms)':<15}")
    output.append("-" * 80)
    for m in latency_measurements:
        output.append(
            f"{m['operation']:<25} | {m['path']:<35} | {m['latency_ms']:<15.2f}"
        )
        stats[m["operation"]].append(m["latency_ms"])
    output.append("-" * 80)
    output.append(f"{'Operation':<25} | {'Count':<5} | {'Mean Latency (ms)':<20}")
    output.append("-" * 80)
    for op, lats in stats.items():
        mean_lat = sum(lats) / len(lats)
        output.append(f"{op:<25} | {len(lats):<5} | {mean_lat:<20.2f}")
    output.append("=" * 80 + "\n")

    out_str = "\n".join(output)
    print(out_str)

    with open("latency_results.txt", "a") as f:
        f.write(out_str + "\n")

    latency_measurements.clear()


BENCHMARK_GROUP = "info"


def _info_op(gcs, path, pattern="info"):
    start_time = time.perf_counter()
    try:
        if pattern == "info":
            gcs.info(path)
        else:
            raise ValueError(f"Unsupported pattern: {pattern}")
    except FileNotFoundError:
        pass
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"{pattern.upper()} : {path} - {duration_ms:.2f} ms.")


def _info_ops(gcs, paths, pattern="info"):
    for path in paths:
        _info_op(gcs, path, pattern)


all_benchmark_cases = get_info_benchmark_cases()
single_threaded_cases, multi_threaded_cases, multi_process_cases = filter_test_cases(
    all_benchmark_cases
)


@pytest.mark.parametrize(
    "gcsfs_benchmark_info",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_info_single_threaded(benchmark, gcsfs_benchmark_info, monitor):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_info

    paths = _get_target_paths(target_dirs, file_paths, params)

    run_single_threaded(
        benchmark,
        monitor,
        params,
        _info_ops,
        (gcs, paths, params.pattern),
        BENCHMARK_GROUP,
    )
    _print_latency_table()


@pytest.mark.parametrize(
    "gcsfs_benchmark_info",
    multi_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_info_multi_threaded(benchmark, gcsfs_benchmark_info, monitor):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_info

    paths = _get_target_paths(target_dirs, file_paths, params)

    run_multi_threaded(
        benchmark,
        monitor,
        params,
        _info_ops,
        (gcs, paths, params.pattern),
        BENCHMARK_GROUP,
    )
    _print_latency_table()


def _get_target_paths(target_dirs, file_paths, params):
    if params.target_type == "bucket":
        return [params.bucket_name]
    elif params.target_type == "folder":
        return target_dirs
    elif params.target_type == "file":
        return file_paths
    else:
        raise ValueError(f"Unsupported target type: {params.target_type}")


def _chunk_list(data, n):
    k, m = divmod(len(data), n)
    return [data[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


def _process_worker(
    gcs, paths, threads, process_durations_shared, index, pattern="info"
):
    """A worker function for each process to run info operations."""
    start_time = time.perf_counter()
    chunks = _chunk_list(paths, threads)
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [
            executor.submit(_info_ops, gcs, chunks[i], pattern) for i in range(threads)
        ]
        [f.result() for f in futures]
    duration_s = time.perf_counter() - start_time
    process_durations_shared[index] = duration_s


@pytest.mark.parametrize(
    "gcsfs_benchmark_info",
    multi_process_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_info_multi_process(
    benchmark, gcsfs_benchmark_info, extended_gcs_factory, request, monitor
):
    gcs, target_dirs, file_paths, prefix, params = gcsfs_benchmark_info

    chunks = _chunk_list(
        _get_target_paths(target_dirs, file_paths, params), params.processes
    )

    def args_builder(gcs_instance, i, shared_arr):
        return (
            gcs_instance,
            chunks[i],
            params.threads,
            shared_arr,
            i,
            params.pattern,
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
