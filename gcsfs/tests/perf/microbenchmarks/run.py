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

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_benchmarks(results_dir, group_name=None, config_name=None, test_name=None):
    """
    Sets environment variables and runs pytest for the specified benchmark group.
    """
    logging.info(f"Starting benchmark run for group: {group_name}")

    # Set environment variables for the benchmark run
    env = os.environ.copy()
    env["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"] = "true"
    env["STORAGE_EMULATOR_HOST"] = "https://storage.googleapis.com"

    # Set benchmark filter if config name is passed
    if config_name:
        env["GCSFS_BENCHMARK_FILTER"] = config_name

    base_path = os.path.dirname(__file__)

    if group_name:
        benchmark_path = os.path.join(base_path, group_name)
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
        "-o",
        "log_cli=true",
    ]

    if test_name:
        pytest_command.append("-k")
        pytest_command.append(test_name)

    logging.info(f"Executing command: {' '.join(pytest_command)}")

    try:
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


def generate_report(json_path, results_dir):
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


def print_csv_to_shell(report_path):
    """
    Reads a CSV file and prints its contents to the shell as a formatted table.
    """
    try:
        with open(report_path, "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
        if not rows:
            logging.info("No data to display.")
            return

        table = PrettyTable()
        table.field_names = rows[0]
        for row in rows[1:]:
            table.add_row(row)
        print(table)
    except FileNotFoundError:
        logging.error(f"Report file not found at: {report_path}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
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
    args = parser.parse_args()

    # Create results directory
    timestamp = datetime.now().strftime("%d%m%Y-%H%M%S")
    results_dir = os.path.join(os.path.dirname(__file__), "__run__", timestamp)
    os.makedirs(results_dir, exist_ok=True)

    # Run benchmarks and generate report
    json_result_path = run_benchmarks(results_dir, args.group, args.config, args.name)
    if json_result_path:
        csv_report_path = generate_report(json_result_path, results_dir)
        if csv_report_path:
            print_csv_to_shell(csv_report_path)
