from dataclasses import dataclass

from gcsfs.tests.perf.microbenchmarks.parameters import BaseBenchmarkParameters


@dataclass
class UrlToFsBenchmarkParameters(BaseBenchmarkParameters):
    """
    Parameters for url_to_fs benchmarks.
    """

    folders: int
