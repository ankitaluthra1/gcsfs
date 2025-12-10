import logging
import statistics
import sys
from typing import Any, Callable, List, Tuple

MB = 1024 * 1024


def publish_benchmark_extra_info(
    benchmark: Any, params: Any, benchmark_group: str
) -> None:
    """
    Helper function to publish benchmark parameters to the extra_info property.
    """
    benchmark.extra_info["num_files"] = params.num_files
    benchmark.extra_info["file_size"] = params.file_size_bytes
    benchmark.extra_info["chunk_size"] = params.chunk_size_bytes
    benchmark.extra_info["block_size"] = params.block_size_bytes
    benchmark.extra_info["pattern"] = params.pattern
    benchmark.extra_info["threads"] = params.num_threads
    benchmark.extra_info["rounds"] = params.rounds
    benchmark.extra_info["bucket_name"] = params.bucket_name
    benchmark.extra_info["bucket_type"] = params.bucket_type
    benchmark.extra_info["processes"] = params.num_processes
    benchmark.group = benchmark_group


def publish_multi_process_benchmark_extra_info(
    benchmark: Any, round_durations_s: List[float], params: Any
) -> None:
    """
    Calculate statistics for multi-process benchmarks and publish them
    to extra_info.
    """
    if not round_durations_s:
        return

    min_time = min(round_durations_s)
    max_time = max(round_durations_s)
    mean_time = statistics.mean(round_durations_s)
    median_time = statistics.median(round_durations_s)
    stddev_time = (
        statistics.stdev(round_durations_s) if len(round_durations_s) > 1 else 0.0
    )

    # Build the results table as a single multi-line string to log it cleanly.
    results_table = (
        f"\n{'-' * 90}\n"
        f"{'Name (time in s)':<50s} {'Min':>8s} {'Max':>8s} {'Mean':>8s} {'Rounds':>8s}\n"
        f"{'-' * 90}\n"
        f"{params.name:<50s} {min_time:>8.4f} {max_time:>8.4f} {mean_time:>8.4f} {params.rounds:>8d}\n"
        f"{'-' * 90}"
    )
    logging.info(f"Multi-process benchmark results:{results_table}")

    benchmark.extra_info["timings"] = round_durations_s
    benchmark.extra_info["min_time"] = min_time
    benchmark.extra_info["max_time"] = max_time
    benchmark.extra_info["mean_time"] = mean_time
    benchmark.extra_info["median_time"] = median_time
    benchmark.extra_info["stddev_time"] = stddev_time


def with_file_sizes(base_cases_func: Callable) -> Callable:
    """
    A decorator that generates benchmark cases for different file sizes.

    It reads file sizes from the BENCHMARK_FILE_SIZES_MB setting and creates
    variants for each specified size, updating the case name and file size parameter.
    """
    from gcsfs.tests.settings import BENCHMARK_FILE_SIZES_MB

    if not BENCHMARK_FILE_SIZES_MB:
        logging.error("No file sizes defined. Please set GCSFS_BENCHMARK_FILE_SIZES.")
        sys.exit(1)

    def wrapper():
        base_cases = base_cases_func()
        new_cases = []
        for case in base_cases:
            for size_mb in BENCHMARK_FILE_SIZES_MB:
                new_case = case.__class__(**case.__dict__)
                new_case.file_size_bytes = size_mb * MB
                new_case.name = f"{case.name}_{size_mb}mb_file"
                new_cases.append(new_case)
        return new_cases

    return wrapper


def with_bucket_types(bucket_configs: List[Tuple[str, str]]) -> Callable:
    """
    A decorator that generates benchmark cases for different bucket types.

    Args:
        bucket_configs: A list of tuples, where each tuple contains the
                        bucket name and a descriptive tag (e.g., "regional").
    """

    def decorator(base_cases_func):
        def wrapper():
            base_cases = base_cases_func()
            all_cases = []
            for case in base_cases:
                for bucket_name, bucket_tag in bucket_configs:
                    if bucket_name:  # Only create cases if bucket is specified
                        new_case = case.__class__(**case.__dict__)
                        new_case.bucket_name = bucket_name
                        new_case.bucket_type = bucket_tag
                        new_case.name = f"{case.name}_{bucket_tag}"
                        all_cases.append(new_case)
            return all_cases

        return wrapper

    return decorator
