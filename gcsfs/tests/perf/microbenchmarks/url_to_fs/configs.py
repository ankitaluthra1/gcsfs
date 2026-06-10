from gcsfs.tests.perf.microbenchmarks.configs import BaseBenchmarkConfigurator
from gcsfs.tests.perf.microbenchmarks.url_to_fs.parameters import (
    UrlToFsBenchmarkParameters,
)


class UrlToFsConfigurator(BaseBenchmarkConfigurator):
    param_class = UrlToFsBenchmarkParameters

    def build_cases(self, scenario, common_config):
        cases = []
        bucket_types = common_config.get("bucket_types", ["zonal"])
        files_list = scenario.get("files", [1])
        threads_list = scenario.get("threads", [1])
        procs_list = scenario.get("processes", [1])
        rounds = scenario.get("rounds", 1)

        for bucket_type in bucket_types:
            bucket_name = self.get_bucket_name(bucket_type)
            if not bucket_name:
                continue

            for files in files_list:
                if "threads" in scenario:
                    for threads in threads_list:
                        name = (
                            f"{scenario['name']}_{threads}threads_"
                            f"{files}files_{bucket_type}"
                        )
                        cases.append(
                            self._create_params(
                                name,
                                bucket_name,
                                bucket_type,
                                threads,
                                1,
                                files,
                                rounds,
                            )
                        )
                elif "processes" in scenario:
                    for procs in procs_list:
                        name = (
                            f"{scenario['name']}_{procs}procs_"
                            f"{files}files_{bucket_type}"
                        )
                        cases.append(
                            self._create_params(
                                name,
                                bucket_name,
                                bucket_type,
                                1,
                                procs,
                                files,
                                rounds,
                            )
                        )
                else:
                    name = (
                        f"{scenario['name']}_1procs_1threads_"
                        f"{files}files_{bucket_type}"
                    )
                    cases.append(
                        self._create_params(
                            name,
                            bucket_name,
                            bucket_type,
                            1,
                            1,
                            files,
                            rounds,
                        )
                    )
        return cases

    def _create_params(
        self,
        name,
        bucket_name,
        bucket_type,
        threads,
        processes,
        files,
        rounds,
    ):
        return self.param_class(
            name=name,
            bucket_name=bucket_name,
            bucket_type=bucket_type,
            threads=threads,
            processes=processes,
            files=files,
            rounds=rounds,
        )


def get_url_to_fs_benchmark_cases():
    return UrlToFsConfigurator(__file__).generate_cases()
