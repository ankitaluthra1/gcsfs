import logging
import random
import time

import pytest

from gcsfs.tests.perf.microbenchmarks.cat_ranges.configs import (
    get_cat_ranges_benchmark_cases,
)
from gcsfs.tests.perf.microbenchmarks.runner import (
    filter_test_cases,
    run_single_threaded,
)

BENCHMARK_GROUP = "cat_ranges"


def _cat_ranges_op(gcs, paths, starts, ends):
    start_time = time.perf_counter()
    gcs.cat_ranges(paths, starts, ends)
    duration_ms = (time.perf_counter() - start_time) * 1000
    logging.info(f"CAT_RANGES: {len(paths)} ranges - {duration_ms:.2f} ms")


all_benchmark_cases = get_cat_ranges_benchmark_cases()
# We only support single threaded for now as cat_ranges is async/batched internally
single_threaded_cases, _, _ = filter_test_cases(all_benchmark_cases)


@pytest.mark.parametrize(
    "gcsfs_benchmark_cat_ranges",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_cat_ranges(benchmark, gcsfs_benchmark_cat_ranges, monitor):
    gcs, file_paths, params = gcsfs_benchmark_cat_ranges

    paths = []
    starts = []
    ends = []

    if params.files == 1:
        # Single file, multiple ranges
        fn = file_paths[0]
        for _ in range(params.num_ranges):
            start = random.randint(0, params.file_size_bytes - params.range_size_bytes)
            paths.append(fn)
            starts.append(start)
            ends.append(start + params.range_size_bytes)
    else:
        # Multiple files. Distribute ranges across files.
        for i in range(params.num_ranges):
            fn = file_paths[i % len(file_paths)]
            start = 0  # Simple start at 0
            paths.append(fn)
            starts.append(start)
            ends.append(start + params.range_size_bytes)

    op_args = (gcs, paths, starts, ends)
    run_single_threaded(
        benchmark, monitor, params, _cat_ranges_op, op_args, BENCHMARK_GROUP
    )