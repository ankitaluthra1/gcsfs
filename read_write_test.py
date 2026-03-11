import asyncio
import os
import statistics
import time

from gcsfs.extended_gcsfs import ExtendedGcsFileSystem

# Placeholders for bucket names
ZONAL_BUCKET = os.getenv("GCSFS_ZONAL_TEST_BUCKET", "yuxinj-gcsfs-test-zonal")
REGIONAL_BUCKET = os.getenv("GCSFS_TEST_BUCKET", "yuxinj-gcsfs-test-hns")

NUM_ITERATIONS = 10


def get_stats(data):
    return {
        "mean": statistics.mean(data),
        "var": statistics.variance(data) if len(data) > 1 else 0.0,
        "min": min(data),
        "max": max(data),
    }


async def benchmark_latency():
    os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"

    zonal_first_write_latencies = []
    zonal_subsequent_write_latencies = []
    zonal_first_read_latencies = []

    print(f"Benchmarking Zonal Bucket: {ZONAL_BUCKET}")
    if "<ZONAL_BUCKET>" in ZONAL_BUCKET:
        print("Skipping zonal benchmark because bucket name is a placeholder.")
    else:

        for i in range(NUM_ITERATIONS):
            fs = ExtendedGcsFileSystem()
            test_file = f"{ZONAL_BUCKET}/latency_test_zonal_{i}.txt"

            # 1. First Write Latency (Lazy initialization of gRPC client and AAOW)
            start = time.perf_counter()
            with fs.open(test_file, "wb") as f:
                f.write(b"Hello Zonal")
            zonal_first_write_latencies.append(time.perf_counter() - start)

            # 2. Subsequent Write Latency (Client and stream already initialized)
            start = time.perf_counter()
            with fs.open(test_file, "wb") as f:
                f.write(b"Hello Zonal Again")
            zonal_subsequent_write_latencies.append(time.perf_counter() - start)

            # 3. First Read Latency (Lazy initialization of gRPC client and MRD)
            # Note: We need a fresh FS object to test true "first read" if we already wrote
            fs_read = ExtendedGcsFileSystem()
            start = time.perf_counter()
            _ = fs_read.cat(test_file)
            zonal_first_read_latencies.append(time.perf_counter() - start)

    print("\n" + "=" * 40 + "\n")

    regional_first_write_latencies = []
    regional_subsequent_write_latencies = []
    regional_first_read_latencies = []

    print(f"Benchmarking Regional Bucket (HNS): {REGIONAL_BUCKET}")
    if "<REGIONAL_BUCKET>" in REGIONAL_BUCKET:
        print("Skipping regional benchmark because bucket name is a placeholder.")
    else:
        test_file = f"{REGIONAL_BUCKET}/latency_test_regional.txt"

        for i in range(NUM_ITERATIONS):
            fs = ExtendedGcsFileSystem()
            # 1. First Write Latency
            start = time.perf_counter()
            with fs.open(test_file, "wb") as f:
                f.write(b"Hello Regional")
            regional_first_write_latencies.append(time.perf_counter() - start)

            # 2. Subsequent Write Latency
            start = time.perf_counter()
            with fs.open(test_file, "wb") as f:
                f.write(b"Hello Regional Again")
            regional_subsequent_write_latencies.append(time.perf_counter() - start)

            # 3. First Read Latency
            fs_read_reg = ExtendedGcsFileSystem()
            start = time.perf_counter()
            fs_read_reg.cat(test_file)
            regional_first_read_latencies.append(time.perf_counter() - start)

    results = []

    def log(msg):
        print(msg)
        results.append(msg)

    log(f"\n--- Zonal Bucket Latency Statistics ({NUM_ITERATIONS} iterations) ---")
    if zonal_first_write_latencies:
        stats = get_stats(zonal_first_write_latencies)
        log(
            f"First Write Latency (Zonal):      Mean: {stats['mean']:.4f}s, "
            f"Var: {stats['var']:.6f}, Min: {stats['min']:.4f}s, Max: {stats['max']:.4f}s"
        )
    if zonal_subsequent_write_latencies:
        stats = get_stats(zonal_subsequent_write_latencies)
        log(
            f"Subsequent Write Latency (Zonal): Mean: {stats['mean']:.4f}s, "
            f"Var: {stats['var']:.6f}, Min: {stats['min']:.4f}s, Max: {stats['max']:.4f}s"
        )
    if zonal_first_read_latencies:
        stats = get_stats(zonal_first_read_latencies)
        log(
            f"First Read Latency (Zonal):       Mean: {stats['mean']:.4f}s, "
            f"Var: {stats['var']:.6f}, Min: {stats['min']:.4f}s, Max: {stats['max']:.4f}s"
        )

    log(f"\n--- Regional Bucket Latency Statistics ({NUM_ITERATIONS} iterations) ---")
    if regional_first_write_latencies:
        stats = get_stats(regional_first_write_latencies)
        log(
            f"First Write Latency (Regional):      Mean: {stats['mean']:.4f}s, "
            f"Var: {stats['var']:.6f}, Min: {stats['min']:.4f}s, Max: {stats['max']:.4f}s"
        )
    if regional_subsequent_write_latencies:
        stats = get_stats(regional_subsequent_write_latencies)
        log(
            f"Subsequent Write Latency (Regional): Mean: {stats['mean']:.4f}s, "
            f"Var: {stats['var']:.6f}, Min: {stats['min']:.4f}s, Max: {stats['max']:.4f}s"
        )
    if regional_first_read_latencies:
        stats = get_stats(regional_first_read_latencies)
        log(
            f"First Read Latency (Regional):       Mean: {stats['mean']:.4f}s, "
            f"Var: {stats['var']:.6f}, Min: {stats['min']:.4f}s, Max: {stats['max']:.4f}s"
        )

    with open("read_write_stats.txt", "w") as f:
        f.write("\n".join(results) + "\n")
    print("\nResults saved to latency_results.txt")


if __name__ == "__main__":
    asyncio.run(benchmark_latency())
