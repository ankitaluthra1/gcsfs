import argparse
import csv
import json
import logging
import os
import subprocess
import sys
from datetime import datetime

import numpy as np
from conftest import MB
from prettytable import PrettyTable


def _setup_environment(args):
    """Validates arguments and sets up environment variables for the benchmark run."""
    # Validate that at least one bucket is provided
    if not any([args.regional_bucket, args.zonal_bucket, args.hns_bucket]):
        logging.error(
            "At least one of --regional-bucket, --zonal-bucket, or --hns-bucket must be provided."
        )
        sys.exit(1)

    # Set environment variables for buckets
    os.environ["GCSFS_TEST_BUCKET"] = (
        args.regional_bucket if args.regional_bucket else ""
    )
    os.environ["GCSFS_ZONAL_TEST_BUCKET"] = (
        args.zonal_bucket if args.zonal_bucket else ""
    )
    os.environ["GCSFS_HNS_TEST_BUCKET"] = args.hns_bucket if args.hns_bucket else ""
    os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"
    os.environ["STORAGE_EMULATOR_HOST"] = "https://storage.googleapis.com"

    if args.config:
        os.environ["GCSFS_BENCHMARK_FILTER"] = args.config

    # Set file sizes from arguments if provided by the user
    if args.file_sizes:
        file_sizes_str = ",".join(map(str, args.file_sizes))
        os.environ["GCSFS_BENCHMARK_FILE_SIZES"] = file_sizes_str


def _run_benchmarks(results_dir, args):
    """
    Sets environment variables and runs pytest for the specified benchmark group.
    """
    logging.info(f"Starting benchmark run for group: {args.group}")

    base_path = os.path.dirname(__file__)
    if args.group:
        benchmark_path = os.path.join(base_path, args.group)
        if not os.path.isdir(benchmark_path):
            logging.error(f"Benchmark group directory not found: {benchmark_path}")
            sys.exit(1)
    else:
        benchmark_path = base_path

    json_output_path = os.path.join(results_dir, "results.json")

    pytest_command = [
        sys.executable,
        "-m",
        "pytest",
        benchmark_path,
        f"--benchmark-json={json_output_path}",
    ]

    if args.log:
        pytest_command.extend(
            [
                "-o",
                f"log_cli={args.log}",
                "-o",
                f"log_cli_level={args.log_level.upper()}",
            ]
        )

    if args.name:
        pytest_command.append("-k")
        pytest_command.append(args.name)

    logging.info(f"Executing command: {' '.join(pytest_command)}")

    try:
        env = os.environ.copy()
        subprocess.run(pytest_command, check=True, env=env, text=True)
        logging.info(f"Benchmark run completed. Results saved to {json_output_path}")
        return json_output_path
    except subprocess.CalledProcessError as e:
        logging.error(f"Pytest execution failed: {e}")
        sys.exit(1)
    except FileNotFoundError:
        logging.error(
            "pytest not found. Please ensure it is installed in your environment."
        )
        sys.exit(1)


def _generate_report(json_path, results_dir):
    """
    Parses the benchmark JSON output and generates a CSV summary report.
    """
    logging.info(f"Generating CSV report from {json_path}")

    with open(json_path, "r") as f:
        data = json.load(f)

    report_path = os.path.join(results_dir, "results.csv")

    # Dynamically get headers from the first benchmark's extra_info and stats
    first_benchmark = data["benchmarks"][0]
    extra_info_headers = sorted(first_benchmark["extra_info"].keys())
    stats_headers = ["min", "max", "mean", "median", "stddev"]
    custom_headers = ["p90", "p95", "p99", "max_throughput_MB/s"]

    headers = ["name", "group"] + extra_info_headers + stats_headers + custom_headers

    with open(report_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for bench in data["benchmarks"]:
            row = {h: "" for h in headers}
            row["name"] = bench["name"]
            row["group"] = bench.get("group", "")

            # Populate extra_info and stats
            for key in extra_info_headers:
                row[key] = bench["extra_info"].get(key)
            for key in stats_headers:
                row[key] = bench["stats"].get(key)

            # Calculate percentiles
            timings = bench["stats"].get("data")
            if timings:
                row["p90"] = np.percentile(timings, 90)
                row["p95"] = np.percentile(timings, 95)
                row["p99"] = np.percentile(timings, 99)

            # Calculate max throughput
            total_bytes = bench["extra_info"].get("file_size", 0) * bench[
                "extra_info"
            ].get("num_files", 1)
            min_time = bench["stats"].get("min")
            if min_time and min_time > 0:
                row["max_throughput_MB/s"] = (total_bytes / min_time) / MB

            writer.writerow([row[h] for h in headers])

    logging.info(f"CSV report generated at {report_path}")

    return report_path


def _print_csv_to_shell(report_path):
    """
    Reads a CSV file and prints its contents to the shell as a formatted table.
    """
    try:
        with open(report_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            logging.info("No data to display.")
            return

        # Define the headers for the output table
        display_headers = [
            "Bucket Type",
            "Group",
            "Pattern",
            "Files",
            "Threads",
            "Processes",
            "File Size (MB)",
            "Chunk Size (MB)",
            "Block Size (MB)",
            "Min Latency (s)",
            "Mean Latency (s)",
            "Max Throughput(MB/s)",
        ]
        table = PrettyTable()
        table.field_names = display_headers

        for row in rows:
            table.add_row(
                [
                    row.get("bucket_type", ""),
                    row.get("group", ""),
                    row.get("pattern", ""),
                    row.get("num_files", ""),
                    row.get("threads", ""),
                    row.get("processes", ""),
                    f"{float(row.get('file_size', 0)) / MB:.2f}",
                    f"{float(row.get('chunk_size', 0)) / MB:.2f}",
                    f"{float(row.get('block_size', 0)) / MB:.2f}",
                    f"{float(row.get('min', 0)):.4f}",
                    f"{float(row.get('mean', 0)):.4f}",
                    row.get("max_throughput_MB/s", ""),
                ]
            )
        print(table)
    except FileNotFoundError:
        logging.error(f"Report file not found at: {report_path}")


def main():
    """Main entry point for the benchmark execution script."""
    parser = argparse.ArgumentParser(description="Run GCSFS performance benchmarks.")
    parser.add_argument(
        "--group",
        help="The benchmark group to run (e.g., 'read'). Runs all if not specified.",
    )
    parser.add_argument(
        "--config", help="The name of the benchmark configuration to run."
    )
    parser.add_argument(
        "--name", help="A keyword to filter tests by name (passed to pytest -k)."
    )
    parser.add_argument(
        "--regional-bucket",
        help="Name of the regional GCS bucket to use for benchmarks.",
    )
    parser.add_argument(
        "--zonal-bucket",
        help="Name of the zonal GCS bucket to use for benchmarks.",
    )
    parser.add_argument(
        "--hns-bucket",
        help="Name of the HNS GCS bucket to use for benchmarks.",
    )
    parser.add_argument(
        "--log",
        default="false",
        help="Enable pytest console logging (log_cli=true).",
    )
    parser.add_argument(
        "--log-level",
        default="DEBUG",
        help="Set pytest console logging level (e.g., DEBUG, INFO, WARNING). Only effective if --log is enabled.",
    )
    parser.add_argument(
        "--file-sizes",
        nargs="+",
        type=int,
        help="List of file sizes in MB to use for benchmarks (e.g., --file-sizes 128 1024). Defaults to 128MB.",
    )
    args = parser.parse_args()

    _setup_environment(args)

    # Create results directory
    timestamp = datetime.now().strftime("%d%m%Y-%H%M%S")
    results_dir = os.path.join(os.path.dirname(__file__), "__run__", timestamp)
    os.makedirs(results_dir, exist_ok=True)

    # Run benchmarks and generate report
    json_result_path = _run_benchmarks(results_dir, args)
    if json_result_path:
        csv_report_path = _generate_report(json_result_path, results_dir)
        if csv_report_path:
            _print_csv_to_shell(csv_report_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    main()
