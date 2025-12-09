from gcsfs.tests.perf.microbenchmarks.conftest import (
    MB,
    create_benchmark_cases_for_bucket,
)
from gcsfs.tests.perf.microbenchmarks.read.parameters import ReadBenchmarkParameters
from gcsfs.tests.perf.microbenchmarks.settings import BENCHMARK_FILTER
from gcsfs.tests.settings import TEST_BUCKET, TEST_ZONAL_BUCKET

# Base configurations for benchmarks. These will be run against each bucket type.
_base_read_benchmark_cases = [
    # Single-threaded sequential read of single 128MB file
    ReadBenchmarkParameters(
        name="read_seq_1thread_128mb_file",
        num_files=1,
        pattern="seq",
    ),
    # Single-threaded random read of single 128MB file
    ReadBenchmarkParameters(
        name="read_rand_1thread_128mb_file",
        num_files=1,
        file_size_bytes=128 * MB,
        pattern="rand",
    ),
    # Multi-threaded sequential read of 16 x 128MB files
    ReadBenchmarkParameters(
        name="read_seq_16threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=16,
        pattern="seq",
    ),
    # Multi-threaded random read of 16 x 128MB files
    ReadBenchmarkParameters(
        name="read_rand_16threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=16,
        pattern="rand",
    ),
    # Multi-process, multi-threaded sequential read of 16 128MB files by 4 processes and 4 threads per process
    ReadBenchmarkParameters(
        name="read_seq_4procs_4threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=4,
        num_processes=4,
        pattern="seq",
    ),
    # Multi-process, multi-threaded random read of 16 x 128MB files by 4 processes and 4 threads per process
    ReadBenchmarkParameters(
        name="read_rand_4procs_4threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=4,
        num_processes=4,
        pattern="rand",
    ),
    # Multi-process, single-threaded sequential read of 16 128MB files by 16 processes
    ReadBenchmarkParameters(
        name="read_seq_16procs_1thread_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=1,
        num_processes=16,
        pattern="seq",
    ),
    # Multi-process, multi-threaded random read of 16 x 128MB files by 16 processes
    ReadBenchmarkParameters(
        name="read_rand_4procs_4threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=1,
        num_processes=16,
        pattern="rand",
    ),
]


def get_read_benchmark_cases():
    """
    Get the full list of test cases from the common configuration with regional and zonal bucket name
    """
    all_cases = create_benchmark_cases_for_bucket(
        _base_read_benchmark_cases, TEST_BUCKET, "regional"
    ) + create_benchmark_cases_for_bucket(
        _base_read_benchmark_cases, TEST_ZONAL_BUCKET, "zonal"
    )

    filter_name = BENCHMARK_FILTER

    if filter_name:
        return [case for case in all_cases if filter_name.lower() in case.name.lower()]
    return all_cases
