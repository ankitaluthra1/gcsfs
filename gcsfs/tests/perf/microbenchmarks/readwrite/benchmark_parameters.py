from dataclasses import dataclass, field
from typing import List


@dataclass
class BenchmarkParameters:
    """
    Defines the parameters for a read write benchmark test case.
    """

    # A friendly name for the test case, used for identification.
    name: str

    # Number of files to create for the benchmark.
    num_files: int

    # Size of each file in bytes.
    file_size_bytes: int

    # Number of threads for multi-threaded tests. 1 for single-threaded.
    num_threads: int

    # The size of each read operation in bytes.
    chunk_size_bytes: int

    # The block size for gcsfs file buffering.
    block_size_bytes: int

    # Read pattern: "seq" for sequential, "rand" for random.
    pattern: str

    # Number of rounds for the benchmark.
    rounds: int

     # The name of the GCS bucket to use for the benchmark.
    bucket_name: str = ""

    # The type of the bucket, e.g., "regional", "zonal", "hns".
    bucket_type: str = ""