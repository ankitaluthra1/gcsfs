import os
import statistics
import time

import fsspec

os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem, zb_hns_utils

# 1. Enforce the experimental flag

buckets = {
    "HNS bucket": "yuxinj-gcsfs-test-hns",
    "Zonal bucket": "yuxinj-gcsfs-test-zonal",
    "Regional bucket": "yuxinj-gcsfs-test",
}

# Monkeypatch
orig_get_grpc_client = ExtendedGcsFileSystem._get_grpc_client
orig_init_mrd = zb_hns_utils.init_mrd

internal_times = {}


async def patched_get_grpc_client(self, *args, **kwargs):
    start = time.perf_counter()
    res = await orig_get_grpc_client(self, *args, **kwargs)
    internal_times["get_grpc_client"] = time.perf_counter() - start
    return res


async def patched_init_mrd(*args, **kwargs):
    start = time.perf_counter()
    res = await orig_init_mrd(*args, **kwargs)
    internal_times["init_mrd"] = time.perf_counter() - start
    return res


ExtendedGcsFileSystem._get_grpc_client = patched_get_grpc_client
zb_hns_utils.init_mrd = patched_init_mrd

results = []
NUM_ITERATIONS = 10

for btype, bucket in buckets.items():

    filepath = f"gs://{bucket}/test-object.txt"

    open_times = []
    grpc_times = []
    mrd_times = []

    print(f"\nTesting {btype}: {filepath} for {NUM_ITERATIONS} iterations...")

    for i in range(NUM_ITERATIONS):
        fs = fsspec.filesystem("gs")

        internal_times.clear()

        start_time = time.perf_counter()
        data = fs.cat_file(filepath)
        cat_time = time.perf_counter() - start_time

        grpc_time = internal_times.get("get_grpc_client", 0.0)
        mrd_time = internal_times.get("init_mrd", 0.0)

        open_times.append(cat_time)
        grpc_times.append(grpc_time)
        mrd_times.append(mrd_time)

    def get_stats(data):
        return {
            "mean": statistics.mean(data),
            "var": statistics.variance(data) if len(data) > 1 else 0.0,
            "min": min(data),
            "max": max(data),
        }

    open_stats = get_stats(open_times)
    grpc_stats = get_stats(grpc_times)
    mrd_stats = get_stats(mrd_times)

    res_str = (
        f"{btype} Latency ({NUM_ITERATIONS} iterations):\n"
        f"  Total cat_file() time    -> Mean: {open_stats['mean']:.4f}s, "
        f"Var: {open_stats['var']:.6f}, Min: {open_stats['min']:.4f}s, Max: {open_stats['max']:.4f}s\n"
        f"  get_grpc_client time -> Mean: {grpc_stats['mean']:.4f}s, "
        f"Var: {grpc_stats['var']:.6f}, Min: {grpc_stats['min']:.4f}s, Max: {grpc_stats['max']:.4f}s\n"
        f"  init_mrd time        -> Mean: {mrd_stats['mean']:.4f}s, "
        f"Var: {mrd_stats['var']:.6f}, Min: {mrd_stats['min']:.4f}s, Max: {mrd_stats['max']:.4f}s\n"
    )
    results.append(res_str)
    print(res_str)

with open("read_stats.txt", "w") as f:
    f.write("\n".join(results) + "\n")
print("\nStats saved to read_stats.txt")
