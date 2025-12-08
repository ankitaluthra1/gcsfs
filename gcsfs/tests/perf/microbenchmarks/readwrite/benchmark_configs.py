from gcsfs.tests.perf.microbenchmarks.readwrite.benchmark_parameters import (
    BenchmarkParameters,
)

from gcsfs.tests.settings import TEST_BUCKET, TEST_ZONAL_BUCKET, TEST_HNS_BUCKET
from gcsfs.tests.perf.microbenchmarks.settings import BENCHMARK_FILTER

MB = 1024 * 1024
GB = 1024 * MB

# Base configurations for benchmarks. These will be run against each bucket type.
_base_read_benchmark_cases = [
    # Single-threaded sequential read of a 1GB file
    BenchmarkParameters(
        name="read_seq_1thread_1gb_file",
        num_files=1,
        file_size_bytes=1 * GB,
        num_threads=1,
        chunk_size_bytes=100 * MB,
        block_size_bytes=100 * MB,
        pattern="seq",
        rounds=5,
    ),
    # Single-threaded random read of a 1GB file
    BenchmarkParameters(
        name="read_rand_1thread_1gb_file",
        num_files=1,
        file_size_bytes=1 * GB,
        num_threads=1,
        chunk_size_bytes=16 * MB,
        block_size_bytes=256 * MB,
        pattern="rand",
        rounds=5,
    ),
    # Multi-threaded sequential read of 16 128MB files
    BenchmarkParameters(
        name="read_seq_16threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=16,
        chunk_size_bytes=16 * MB,
        block_size_bytes=128 * MB,
        pattern="seq",
        rounds=5,
    ),
    # Multi-threaded random read of 16 128MB files
    BenchmarkParameters(
        name="read_rand_16threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=16,
        chunk_size_bytes=16 * MB,
        block_size_bytes=128 * MB,
        pattern="rand",
        rounds=5,
    ),
]

def get_read_benchmark_cases():
    """
    Get the full list of test cases from the common configuration with bucket name
    """
    all_cases = (
        _create_benchmark_cases_for_bucket(_base_read_benchmark_cases, TEST_BUCKET, "regional")
        + _create_benchmark_cases_for_bucket(_base_read_benchmark_cases, TEST_ZONAL_BUCKET, "zonal")
        + _create_benchmark_cases_for_bucket(_base_read_benchmark_cases, TEST_HNS_BUCKET, "hns")
    )

    filter_name = BENCHMARK_FILTER

    if filter_name:
        return [case for case in all_cases if filter_name.lower() in case.name.lower()]
    return all_cases

def _create_benchmark_cases_for_bucket(base_cases, bucket_name, bucket_tag):
    new_cases = []

    if not bucket_name:
        return new_cases

    for case in base_cases:
        new_case = case.__class__(**case.__dict__)
        new_case.bucket_name = bucket_name
        new_case.bucket_type = bucket_tag
        new_case.name = f"{case.name}_{bucket_tag}"
        new_cases.append(new_case)
    return new_cases