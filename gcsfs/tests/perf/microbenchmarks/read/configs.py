from gcsfs.tests.perf.microbenchmarks.conftest import create_benchmark_cases_for_bucket
from gcsfs.tests.perf.microbenchmarks.read.parameters import ReadBenchmarkParameters
from gcsfs.tests.perf.microbenchmarks.settings import BENCHMARK_FILTER
from gcsfs.tests.settings import TEST_BUCKET, TEST_HNS_BUCKET, TEST_ZONAL_BUCKET

MB = 1024 * 1024
GB = 1024 * MB

# Base configurations for benchmarks. These will be run against each bucket type.
_base_read_benchmark_cases = [
    # Single-threaded sequential read of single 128MB file
    ReadBenchmarkParameters(
        name="read_seq_1thread_128mb_file",
        num_files=1,
        file_size_bytes=128 * MB,
        chunk_size_bytes=16 * MB,
        block_size_bytes=16 * MB,
        pattern="seq",
    ),
    # Single-threaded random read of single 128MB file
    ReadBenchmarkParameters(
        name="read_rand_1thread_128mb_file",
        num_files=1,
        file_size_bytes=128 * MB,
        chunk_size_bytes=16 * MB,
        block_size_bytes=16 * MB,
        pattern="rand",
    ),
    # Multi-threaded sequential read of four 128MB files
    ReadBenchmarkParameters(
        name="read_seq_16threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=16,
        chunk_size_bytes=16 * MB,
        block_size_bytes=16 * MB,
        pattern="seq",
    ),
    # Multi-threaded random read of four 128MB files
    ReadBenchmarkParameters(
        name="read_rand_16threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=16,
        chunk_size_bytes=16 * MB,
        block_size_bytes=16 * MB,
        pattern="rand",
    ),
    # Multi-threaded random read of single 128MB file by 4 parallel threads
    ReadBenchmarkParameters(
        name="read_rand_16threads_128mb_file",
        num_files=1,
        file_size_bytes=128 * MB,
        num_threads=16,
        chunk_size_bytes=16 * MB,
        block_size_bytes=16 * MB,
        pattern="rand",
    ),
    # Multi-process, multi-threaded sequential read of 16 128MB files by 4 processes and 4 threads per process
    ReadBenchmarkParameters(
        name="read_seq_4procs_4threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=4,
        num_processes=4,
        chunk_size_bytes=16 * MB,
        block_size_bytes=16 * MB,
        pattern="seq",
    ),
    # Multi-process, multi-threaded random read of 16 128MB files by 4 processes and 4 threads per process
    ReadBenchmarkParameters(
        name="read_rand_4procs_4threads_16x128mb_files",
        num_files=16,
        file_size_bytes=128 * MB,
        num_threads=4,
        num_processes=4,
        chunk_size_bytes=16 * MB,
        block_size_bytes=16 * MB,
        pattern="rand",
    ),
    # Multi-process, multi-threaded random read of a single 128MB file by 4 processes and 4 threads per process
    ReadBenchmarkParameters(
        name="read_rand_4procs_4threads_128mb_file",
        num_files=1,
        file_size_bytes=128 * MB,
        num_threads=4,
        num_processes=4,
        chunk_size_bytes=16 * MB,
        block_size_bytes=16 * MB,
        pattern="rand",
    ),
]


def get_read_benchmark_cases():
    """
    Get the full list of test cases from the common configuration with bucket name
    """
    all_cases = (
        create_benchmark_cases_for_bucket(
            _base_read_benchmark_cases, TEST_BUCKET, "regional"
        )
        + create_benchmark_cases_for_bucket(
            _base_read_benchmark_cases, TEST_ZONAL_BUCKET, "zonal"
        )
        + create_benchmark_cases_for_bucket(
            _base_read_benchmark_cases, TEST_HNS_BUCKET, "hns"
        )
    )

    filter_name = BENCHMARK_FILTER

    if filter_name:
        return [case for case in all_cases if filter_name.lower() in case.name.lower()]
    return all_cases
