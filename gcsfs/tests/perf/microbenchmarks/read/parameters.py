from dataclasses import dataclass


@dataclass
class ReadBenchmarkParameters:
    """
    Defines the parameters for a read benchmark test cases.
    """

    # The name of config
    name: str

    # The block size for gcsfs file buffering.
    block_size_bytes: int

    # The size of each read or write operation in bytes.
    chunk_size_bytes: int

    # Size of each file in bytes.
    file_size_bytes: int

    # Number of files to create for the benchmark.
    num_files: int

    # Read pattern: "seq" for sequential, "rand" for random.
    pattern: str

    # The name of the GCS bucket to use for the benchmark.
    bucket_name: str = ""

    # The type of the bucket, e.g., "regional", "zonal", "hns".
    bucket_type: str = ""

    # Number of threads for multi-threaded tests. 1 for single-threaded.
    num_threads: int = 1

    # Number of processes for multi-process tests.
    num_processes: int = 1

    # Number of rounds for the benchmark, default to 10.
    rounds: int = 10
