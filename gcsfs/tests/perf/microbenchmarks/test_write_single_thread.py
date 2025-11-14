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


def test_write_files(benchmark, gcs_write_benchmark_fixture):
    """Benchmark for writing a number of files sequentially."""
    gcs, file_paths, data, num_files, file_size = gcs_write_benchmark_fixture
    logging.info(
        f"Running write_files benchmark for {num_files} files of size {file_size // 1024}KB."
    )
    benchmark.group = f"write_files_{num_files}_files_{file_size // 1024}kb"

    def write_op(paths, content):
        for path in paths:
            with gcs.open(path, "wb") as f:
                f.write(content)

    benchmark(write_op, paths=file_paths, content=data)


def _setup(args):
    """Prepare file paths and content for the write benchmark."""
    print("--- Preparing for benchmark ---")
    file_paths = [f"{args.bucket}/{uuid.uuid4()}" for _ in range(args.num_files)]
    file_size_bytes = args.file_size_kb * 1024
    file_content = uuid.uuid4().hex.encode() * (file_size_bytes // 32 + 1)
    print(
        f"Will write {args.num_files} objects of {args.file_size_kb}KB each to bucket {args.bucket}"
    )
    return file_paths, file_content


def _run_benchmarks(gcs, args, file_paths, file_content):
    """Run the write benchmark and return the results."""
    results = []

    # Benchmark Write
    start_time_write = time.time()
    for path in file_paths:
        with gcs.open(path, "wb") as f:
            f.write(file_content)
    end_time_write = time.time()
    time_taken_write = end_time_write - start_time_write
    print("\n--- Write Result ---")
    print(f"Writing {len(file_paths)} objects took: {time_taken_write:.4f} seconds")
    results.append(
        {
            "Benchmark": "write_files",
            "Project ID": args.project,
            "Bucket ID": args.bucket,
            "Start Time": datetime.fromtimestamp(start_time_write).isoformat(),
            "Time Taken (s)": f"{time_taken_write:.4f}",
            "Num Files": args.num_files,
            "File Size (KB)": args.file_size_kb,
        }
    )
    return results


def _teardown(gcs, file_paths):
    """Delete the files created by the benchmark."""
    gcs.rm(file_paths)
    print(f"\n--- Teardown complete: Deleted {len(file_paths)} objects ---")


def main():
    """Main function to run the benchmark from the command line."""
    parser = get_benchmark_parser("Single-threaded write benchmark for gcsfs.")
    args = parser.parse_args()
    print(f"Running write benchmark for project: {args.project}, bucket: {args.bucket}")

    gcs = get_gcs_filesystem(args.project)
    file_paths, file_content = _setup(args)
    results = _run_benchmarks(gcs, args, file_paths, file_content)
    _teardown(gcs, file_paths)
    print_benchmark_summary(results)


if __name__ == "__main__":
    main()