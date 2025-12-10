from gcsfs.tests.perf.microbenchmarks.benchmark_helpers import (
    with_bucket_types,
    with_file_sizes,
)
from gcsfs.tests.perf.microbenchmarks.read.parameters import ReadBenchmarkParameters
from gcsfs.tests.settings import BENCHMARK_FILTER, TEST_BUCKET

# Base configurations for benchmarks.
# These will be run against each bucket type and various file sizes using the decorators.
_base_read_benchmark_cases = [
    # Single-threaded sequential read
    ReadBenchmarkParameters(
        name="read_seq_1thread",
        num_files=1,
        pattern="seq",
    ),
    # Single-threaded random read
    ReadBenchmarkParameters(
        name="read_rand_1thread",
        num_files=1,
        pattern="rand",
    ),
    # Multi-threaded sequential read
    ReadBenchmarkParameters(
        name="read_seq_16threads_16files",
        num_files=16,
        num_threads=16,
        pattern="seq",
    ),
    # Multi-threaded random read
    ReadBenchmarkParameters(
        name="read_rand_16threads_16files",
        num_files=16,
        num_threads=16,
        pattern="rand",
    ),
    # Multi-process, multi-threaded sequential read
    ReadBenchmarkParameters(
        name="read_seq_4procs_4threads_16files",
        num_files=16,
        num_threads=4,
        num_processes=4,
        pattern="seq",
    ),
    # Multi-process, multi-threaded random read
    ReadBenchmarkParameters(
        name="read_rand_4procs_4threads_16files",
        num_files=16,
        num_threads=4,
        num_processes=4,
        pattern="rand",
    ),
    # Multi-process, single-threaded sequential read
    ReadBenchmarkParameters(
        name="read_seq_16procs_1thread_16files",
        num_files=16,
        num_threads=1,
        num_processes=16,
        pattern="seq",
    ),
    # Multi-process, single-threaded random read
    ReadBenchmarkParameters(
        name="read_rand_16procs_1thread_16files",
        num_files=16,
        num_threads=1,
        num_processes=16,
        pattern="rand",
    ),
]


@with_file_sizes
@with_bucket_types([(TEST_BUCKET, "regional")])
def get_read_benchmark_cases():
    """
    Generates the full list of read benchmark test cases by applying
    file size and bucket type variations to a set of base configurations.
    """
    all_cases = _base_read_benchmark_cases
    filter_name = BENCHMARK_FILTER

    if filter_name:
        return [case for case in all_cases if filter_name.lower() in case.name.lower()]
    return all_cases
