import itertools

from gcsfs.tests.perf.microbenchmarks.cat_ranges.parameters import (
    CatRangesBenchmarkParameters,
)
from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.conftest import KB, MB


class CatRangesConfigurator(BaseBenchmarkConfigurator):
    def build_cases(self, scenario, common_config):
        procs_list = scenario.get("processes", [1])
        threads_list = scenario.get("threads", [1])
        rounds = common_config.get("rounds", 1)
        bucket_types = common_config.get("bucket_types", ["regional"])
        file_sizes_mb = common_config.get("file_sizes_mb", [5])
        num_ranges_list = scenario.get("num_ranges", [100])
        range_sizes_kb = scenario.get("range_sizes_kb", [1])

        cases = []
        param_combinations = itertools.product(
            procs_list,
            threads_list,
            file_sizes_mb,
            bucket_types,
            num_ranges_list,
            range_sizes_kb,
        )

        for procs, threads, size_mb, bucket_type, num_ranges, range_size_kb in param_combinations:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            name = f"{scenario['name']}_{procs}procs_{threads}threads_{size_mb}MB_file_{num_ranges}ranges_{range_size_kb}KB_range_{bucket_type}"

            params = CatRangesBenchmarkParameters(
                name=name,
                bucket_name=bucket_name,
                bucket_type=bucket_type,
                threads=threads,
                processes=procs,
                files=scenario.get("files", 1),
                rounds=rounds,
                file_size_bytes=int(size_mb * MB),
                num_ranges=num_ranges,
                range_size_bytes=int(range_size_kb * KB),
            )
            cases.append(params)
        return cases


def get_cat_ranges_benchmark_cases():
    return CatRangesConfigurator(__file__).generate_cases()