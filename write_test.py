import asyncio
import functools
import os
import statistics
import time

from gcsfs import zb_hns_utils
from gcsfs.extended_gcsfs import ExtendedGcsFileSystem

# Global to store timing
init_aaow_time = 0
NUM_ITERATIONS = 10


def instrument_init_aaow(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        global init_aaow_time
        start = time.perf_counter()
        result = await func(*args, **kwargs)
        end = time.perf_counter()
        init_aaow_time = end - start
        return result

    return wrapper


# Apply instrumentation
zb_hns_utils.init_aaow = instrument_init_aaow(zb_hns_utils.init_aaow)

ZONAL_BUCKET = "yuxinj-gcsfs-test-zonal"
REGIONAL_BUCKET = "yuxinj-gcsfs-test-hns"


def get_stats(data):
    if not data:
        return {"mean": 0, "var": 0, "min": 0, "max": 0}
    return {
        "mean": statistics.mean(data),
        "var": statistics.variance(data) if len(data) > 1 else 0.0,
        "min": min(data),
        "max": max(data),
    }


async def benchmark_combined(bucket, label, results_log):
    global init_aaow_time

    init_aaow_latencies = []
    total_pipe_latencies = []

    print(f"Testing {label} (Bucket: {bucket}) - {NUM_ITERATIONS} iterations")

    for i in range(NUM_ITERATIONS):
        fs = ExtendedGcsFileSystem()
        init_aaow_time = 0  # Reset
        path = f"{bucket}/test_combined_{i}.txt"

        try:
            start_pipe = time.perf_counter()
            with fs.open(path, "wb") as f:
                f.write(b"Hello Zonal")
            end_pipe = time.perf_counter()

            total_pipe_latencies.append(end_pipe - start_pipe)
            init_aaow_latencies.append(init_aaow_time)

        except Exception as e:
            print(f"  Iteration {i} failed: {e}")

    # Calculate stats
    pipe_stats = get_stats(total_pipe_latencies)
    init_stats = get_stats(init_aaow_latencies)

    # Format output
    output_lines = [
        f"\n--- {label} Statistics ({NUM_ITERATIONS} iterations) ---",
        f"Total _pipe_file Latency: Mean: {pipe_stats['mean']:.4f}s, "
        f"Var: {pipe_stats['var']:.6f}, Min: {pipe_stats['min']:.4f}s, Max: {pipe_stats['max']:.4f}s",
        f"init_aaow Latency:        Mean: {init_stats['mean']:.4f}s, "
        f"Var: {init_stats['var']:.6f}, Min: {init_stats['min']:.4f}s, Max: {init_stats['max']:.4f}s",
    ]

    report = "\n".join(output_lines)
    print(report)
    results_log.append(report)


async def main():
    os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"
    results = []

    await benchmark_combined(ZONAL_BUCKET, "Zonal Bucket", results)
    await benchmark_combined(REGIONAL_BUCKET, "Regional HNS Bucket", results)

    with open("write_stats.txt", "w") as f:
        f.write("\n".join(results) + "\n")
    print("\nStats saved to write_stats.txt")


if __name__ == "__main__":
    asyncio.run(main())
