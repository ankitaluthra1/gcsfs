# GCSFS Single-Threaded Microbenchmarks

This document provides instructions on how to run the single-threaded microbenchmarks for `gcsfs`. These benchmarks are designed to measure the performance of fundamental GCS operations (listing, reading, and writing objects) in a controlled, single-threaded environment.

## Overview

The suite consists of `pytest` tests that use the `pytest-benchmark` plugin to measure performance. A shell script, `run_benchmarks.sh`, is provided to automate the entire process, from running the tests to processing and displaying the results.

The primary operations benchmarked are:
- **LIST_OBJECTS**: Measures the latency of `gcs.ls()`.
- **READ_OBJECTS**: Measures the time taken to read a set of files sequentially using `gcs.cat()`.
- **WRITE_OBJECTS**: Measures the time taken to write a set of files sequentially using `gcs.open()`.

## Prerequisites

1.  **Linux Environment**: The runner script is designed for Linux and automatically installs dependencies using `apt-get` (Debian/Ubuntu) or `yum` (CentOS/RHEL). It may require `sudo` privileges to install `jq`.
2.  **Python Environment**: You must have a Python environment with the required packages installed, including `gcsfs`, `pytest`, and `pytest-benchmark`.
3.  **GCP Authentication**: You need to be authenticated with Google Cloud. For local development, the recommended method is:
    ```bash
    gcloud auth application-default login
    ```

## Tools Used

- **pytest-benchmark**: A `pytest` fixture for benchmarking code. It handles the statistical analysis of timing results.
- **jq**: A lightweight and flexible command-line JSON processor. The runner script uses `jq` to parse the raw JSON output from `pytest-benchmark` and generate a formatted summary table.

## The `run_benchmarks.sh` Script

The `run_benchmarks.sh` script, located in the parent `perf` directory, is the main entry point for executing the benchmarks. It handles the following tasks:

1.  **Argument Parsing**: Accepts command-line arguments to configure the benchmark run.
2.  **Dependency Check**: Automatically checks if `jq` is installed and attempts to install it if it's missing.
3.  **Cleanup**: Removes results from previous runs.
4.  **Execution**: Runs the `pytest` benchmarks with the specified configuration, outputting the raw data to `benchmark_results.json`.
5.  **Result Processing**: Parses the JSON output to calculate throughput and tail latencies (P90, P95, P99).
6.  **Output Generation**: Saves the formatted results to `benchmark_results.tsv` and prints a summary table to the console.

## How to Run the Benchmarks

All commands should be run from the `microbenchmarks` directory.

1.  **Make the script executable** (only needs to be done once):
    ```bash
    chmod +x ../run_benchmarks.sh
    ```

2.  **Execute the script** with the required parameters.

### Parameters

The script accepts the following command-line arguments:

| Flag | Description                                  | Mandatory | Default Value                     |
| :--- | :------------------------------------------- | :-------- | :-------------------------------- |
| `-b` | The GCS bucket to use for the benchmark.     | **Yes**   | N/A                               |
| `-p` | The GCP project ID of the bucket.            | **Yes**   | N/A                               |
| `-n` | The number of files to read/write.           | No        | 1                                 |
| `-s` | The size of each file in Megabytes (MB).     | No        | 1                                 |
| `-r` | The number of benchmark rounds to execute.   | No        | 10                                |
| `-i` | The number of iterations within each round.  | No        | 1                                 |
| `-c` | Chunk size for I/O in Megabytes (MB).        | No        | 16                                |
| `--profile` | Enable cProfile for the benchmark run.       | No        | Disabled                          |
| `-h` | Displays the help message.                   | No        | N/A                               |

### Example Commands

**Basic Run:**
This command runs the benchmarks against the specified bucket and project using the default settings (1 file, 1MB size, 10 rounds, 1 iteration).

```bash
../run_benchmarks.sh -b your-gcs-bucket -p your-gcp-project
```

**Customized Run:**
This command runs a more specific benchmark scenario with 100 files of 10MB each, executing for 50 rounds.

```bash
../run_benchmarks.sh -b your-gcs-bucket -p your-gcp-project -n 100 -s 10 -r 50
```

## Output

After a successful run, the script will generate two files in the project's root directory:
- `benchmark_results.json`: The raw, detailed output from `pytest-benchmark`.
- `benchmark_results.tsv`: A tab-separated file containing the consolidated summary table, which is also printed to the console.
