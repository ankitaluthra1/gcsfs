import logging
import os
import time
import uuid
from datetime import datetime

from gcsfs.tests.perf.microbenchmarks.conftest import (
    get_benchmark_parser,
    get_gcs_filesystem,
    print_benchmark_summary,
)
from gcsfs.tests.settings import TEST_BUCKET, TEST_PROJECT


def test_list_objects(benchmark, gcs_read_benchmark_fixture):
    """Benchmark for listing objects within the test bucket."""
    gcs, _, num_files, file_size = gcs_read_benchmark_fixture
    logging.info(
        f"Running list_objects benchmark for {num_files} files of size {file_size // 1024}KB."
    )
    # Set a benchmark group identifier
    benchmark.group = f"list_objects_{num_files}_files_{file_size // 1024}kb"
    benchmark(gcs.ls, TEST_BUCKET)


def test_read_files(benchmark, gcs_read_benchmark_fixture):
    """Benchmark for reading a list of files one by one."""
    gcs, file_paths, num_files, file_size = gcs_read_benchmark_fixture
    logging.info(
        f"Running read_files benchmark for {num_files} files of size {file_size // 1024}KB."
    )
    # Set a benchmark group identifier
    benchmark.group = f"read_files_{num_files}_files_{file_size // 1024}kb"
    benchmark(lambda: [gcs.cat(path) for path in file_paths])


def _setup(gcs, args):
    """Create files for the benchmark and return their paths and content."""
    print("--- Setting up benchmark environment ---")
    file_paths = [f"{args.bucket}/{uuid.uuid4()}" for _ in range(args.num_files)]
    file_size_bytes = args.file_size_kb * 1024
    file_content = uuid.uuid4().hex.encode() * (file_size_bytes // 32 + 1)
    for path in file_paths:
        with gcs.open(path, "wb") as f:
            f.write(file_content)
    print(
        f"Created {args.num_files} objects of {args.file_size_kb}KB each in bucket {args.bucket}"
    )
    return file_paths


def _run_benchmarks(gcs, args, file_paths):
    """Run the benchmarks and return the results."""
    results = []

    # Benchmark ls
    start_time_ls = time.time()
    list_result = gcs.ls(args.bucket)
    end_time_ls = time.time()
    time_taken_ls = end_time_ls - start_time_ls
    print("\n--- LS Result ---")
    print(f"Found {len(list_result)} objects.")
    print(f"Listing objects took: {time_taken_ls:.4f} seconds")
    results.append(
        {
            "Benchmark": "list_objects",
            "Project ID": args.project,
            "Bucket ID": args.bucket,
            "Start Time": datetime.fromtimestamp(start_time_ls).isoformat(),
            "Time Taken (s)": f"{time_taken_ls:.4f}",
            "Num Files": args.num_files,
            "File Size (KB)": args.file_size_kb,
        }
    )

    # Benchmark read
    start_time_read = time.time()
    for path in file_paths:
        gcs.cat(path)
    end_time_read = time.time()
    time_taken_read = end_time_read - start_time_read
    print("\n--- Read Result ---")
    print(f"Reading {len(file_paths)} objects took: {time_taken_read:.4f} seconds")
    results.append(
        {
            "Benchmark": "read_files",
            "Project ID": args.project,
            "Bucket ID": args.bucket,
            "Start Time": datetime.fromtimestamp(start_time_read).isoformat(),
            "Time Taken (s)": f"{time_taken_read:.4f}",
            "Num Files": args.num_files,
            "File Size (KB)": args.file_size_kb,
        }
    )
    return results


def _teardown(gcs, file_paths):
    """Delete the files created for the benchmark."""
    gcs.rm(file_paths)
    print(f"\n--- Teardown complete: Deleted {len(file_paths)} objects ---")


def main():
    """Main function to run the benchmark from the command line."""
    parser = get_benchmark_parser("Single-threaded read benchmark for gcsfs.")
    args = parser.parse_args()
    print(f"Running benchmark for project: {args.project}, bucket: {args.bucket}")

    gcs = get_gcs_filesystem(args.project)
    file_paths = _setup(gcs, args)
    results = _run_benchmarks(gcs, args, file_paths)
    _teardown(gcs, file_paths)
    print_benchmark_summary(results)


if __name__ == "__main__":
    main()