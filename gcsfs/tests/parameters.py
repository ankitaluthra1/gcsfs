from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import IOBenchmarkParameters


@dataclass
class CatRangesBenchmarkParameters(IOBenchmarkParameters):
    """
    Defines the parameters for cat_ranges benchmark test cases.
    """
    num_ranges: int
    range_size_bytes: int