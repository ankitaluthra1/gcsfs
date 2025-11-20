# GCSFS Single-Threaded Microbenchmarks

This document provides instructions on how to run the single-threaded microbenchmarks for `gcsfs`. These benchmarks are designed to measure the performance of fundamental GCS operations (listing, reading, and writing objects) in a controlled, single-threaded environment.

## Overview

The suite consists of `pytest` tests that use the `pytest-benchmark` plugin to measure performance. A shell script, `run_benchmarks.sh` (located in the parent `perf` directory), orchestrates the entire process, from running the tests to processing and displaying the results.

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

## How to Run the Benchmarks

The main entry point is the `run_benchmarks.sh` script located in the parent `perf` directory. It reads the `benchmark_config.yaml` file to determine which scenarios to run.

All commands should be run from the `gcsfs/tests/perf` directory.

1.  **Make the script executable** (only needs to be done once):
    ```bash
    chmod +x run_benchmarks.sh execute_scenario.sh
    ```

2.  **Execute the script**.

    To run all scenarios defined in `benchmark_config.yaml`:
    ```bash
    ./run_benchmarks.sh
    ```
    To run a single, specific scenario from the config file:
    ```bash
    ./run_benchmarks.sh -s read_1gb_file_100mb_chunk
    ```

## Output

After a successful run, the script will generate two files in the project's root directory:
- `benchmark_results.json`: The raw, detailed output from `pytest-benchmark`.
- `benchmark_results.tsv`: A tab-separated file containing the consolidated summary table, which is also printed to the console.
