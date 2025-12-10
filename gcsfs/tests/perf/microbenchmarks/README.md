# GCSFS Microbenchmarks

## Introduction

This document describes the microbenchmark suite for `gcsfs`. These benchmarks are designed to measure the performance of various I/O operations under different conditions. They are built using `pytest` and the `pytest-benchmark` plugin to provide detailed performance metrics for single-threaded, multi-threaded, and multi-process scenarios.

## Prerequisites

Before running the benchmarks, ensure you have the necessary packages installed. The required packages are listed in `gcsfs/tests/perf/microbenchmarks/requirements.txt`.

You can install them using pip:

```bash
pip install -r gcsfs/tests/perf/microbenchmarks/requirements.txt
```

This will install `pytest`, `pytest-benchmark`, and other necessary dependencies. For more information on `pytest-benchmark`, you can refer to its official documentation. [1]

## Read Benchmarks

The read benchmarks are located in `gcsfs/tests/perf/microbenchmarks/read/` and are designed to test read performance with various configurations.

### Parameters

The read benchmarks are defined by the `ReadBenchmarkParameters` class in `read/parameters.py`. Key parameters include:

*   `name`: The name of the benchmark configuration.
*   `num_files`: The number of files to use.
*   `pattern`: Read pattern, either sequential (`seq`) or random (`rand`).
*   `num_threads`: Number of threads for multi-threaded tests.
*   `num_processes`: Number of processes for multi-process tests.
*   `block_size_bytes`: The block size for gcsfs file buffering.
*   `chunk_size_bytes`: The size of each read operation.
*   `file_size_bytes`: The total size of each file.

### Configurations

The benchmarks are split into three main configurations based on the execution model:

*   **Single-threaded (`test_read_single_threaded`)**: Measures baseline performance of read operations on a single file.
*   **Multi-threaded (`test_read_multi_threaded`)**: Measures performance with multiple threads reading from one or more files.
*   **Multi-process (`test_read_multi_process`)**: Measures performance using multiple processes, each with its own set of threads, to test parallelism.

These base configurations are defined in `read/configs.py`. They are then parameterized using decorators to run against different bucket types and file sizes.

*   `@with_bucket_types`: This decorator creates benchmark variants for different GCS bucket types (e.g., regional, zonal).
*   `@with_file_sizes`: This decorator creates variants for different file sizes, which are configured via the `GCSFS_BENCHMARK_FILE_SIZES` environment variable.

### Environment Setup for `pytest`

When running benchmarks directly with `pytest`, you must set the following environment variables. The orchestrator script (`run.py`) handles this automatically.

```bash
export STORAGE_EMULATOR_HOST="https://storage.googleapis.com"
export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT="true"
```

These ensure that the benchmarks run against the live GCS API and that experimental features are enabled.

### Running Benchmarks with `pytest`

You can use `pytest` to run the benchmarks directly. The `-k` option is useful for filtering tests by name.

**Examples:**

Run all read benchmarks:
```bash
pytest gcsfs/tests/perf/microbenchmarks/read/
```

Run only single-threaded read benchmarks:
```bash
pytest -k "test_read_single_threaded" gcsfs/tests/perf/microbenchmarks/read/
```

Run multi-process benchmarks for a specific configuration (e.g., 4 processes, 4 threads):
```bash
pytest -k "read_seq_4procs_4threads" gcsfs/tests/perf/microbenchmarks/read/
```

Run a specific benchmark configuration by setting `GCSFS_BENCHMARK_FILTER`. This is useful for targeting a single configuration defined in `read/configs.py`.
```bash
export GCSFS_BENCHMARK_FILTER="read_seq_1thread"
pytest -k "read_seq_4procs_4threads" gcsfs/tests/perf/microbenchmarks/read/
```

## Function-level Fixture: `gcsfs_benchmark_read_write`

A function-level `pytest` fixture named `gcsfs_benchmark_read_write` (defined in `conftest.py`) is used to set up and tear down the environment for the benchmarks.

### Setup and Teardown

*   **Setup**: Before a benchmark function runs, this fixture creates the specified number of files with the configured size in a temporary directory within the test bucket. It uses `os.urandom()` to write data in chunks to avoid high memory usage.
*   **Teardown**: After the benchmark completes, the fixture recursively deletes the temporary directory and all the files created during the setup phase.

Here is how the fixture is used in a test:

```python
@pytest.mark.parametrize(
    "gcsfs_benchmark_read_write",
    single_threaded_cases,
    indirect=True,
    ids=lambda p: p.name,
)
def test_read_single_threaded(benchmark, gcsfs_benchmark_read_write):
    gcs, file_paths, params = gcsfs_benchmark_read_write
    # ... benchmark logic ...
```

## Settings

To run the benchmarks, you need to configure your environment.

### Environment Variables

The orchestrator script (`run.py`) sets these for you, but if you are running `pytest` directly, you will need to export them.

*   `GCSFS_TEST_BUCKET`: The name of a regional GCS bucket.
*   `GCSFS_ZONAL_TEST_BUCKET`: The name of a zonal GCS bucket.
*   `GCSFS_HNS_TEST_BUCKET`: The name of an HNS-enabled GCS bucket.

### `settings.py`

The `gcsfs/tests/perf/microbenchmarks/settings.py` file defines how benchmark parameters can be configured through environment variables:

*   `GCSFS_BENCHMARK_FILTER`: A string used to filter which benchmark configurations to run by name.
*   `GCSFS_BENCHMARK_FILE_SIZES`: A comma-separated list of file sizes in MB (e.g., "128,1024"). Defaults to "128".

## Orchestrator Script (`run.py`)

An orchestrator script, `run.py`, is provided to simplify running the benchmark suite. It wraps `pytest`, sets up the necessary environment variables, and generates a summary report.

### Parameters

The script accepts several command-line arguments:

*   `--group`: The benchmark group to run (e.g., `read`).
*   `--config`: The name of a specific benchmark configuration to run.
*   `--name`: A keyword to filter tests by name (passed to `pytest -k`).
*   `--regional-bucket`: Name of the Regional GCS bucket.
*   `--zonal-bucket`: Name of the Zonal GCS bucket.
*   `--hns-bucket`: Name of the HNS GCS bucket.
*   `--file-sizes`: A space-separated list of file sizes in MB (e.g., `--file-sizes 128 1024`).
*   `--log`: Set to `true` to enable `pytest` console logging.
*   `--log-level`: Sets the log level (e.g., `INFO`, `DEBUG`).

**Important Notes:**
*   You must provide at least one bucket name (`--regional-bucket`, `--zonal-bucket`, or `--hns-bucket`).
*   If the `--file-sizes` argument is not provided, the script will default to using a 128MB file size for all benchmarks.

Run the script with `--help` to see all available options:
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --help
```

### Examples

Here are some examples of how to use the orchestrator script from the root of the `gcsfs` repository:

Run all available benchmarks against a regional bucket with default settings. This is the simplest way to trigger all tests across all groups (e.g., read, write):
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --regional-bucket your-regional-bucket
```

Run only the `read` group benchmarks against a regional bucket with the default 128MB file size:
```bash
python gcsfs/tests/perf/microbenchmarks/run.py --group read --regional-bucket your-regional-bucket
```

Run only the single-threaded sequential read benchmark with 256MB and 512MB file sizes:
```bash
python gcsfs/tests/perf/microbenchmarks/run.py \
  --group read \
  --name "read_seq_1thread" \
  --regional-bucket your-regional-bucket \
  --file-sizes 256 512
```

Run all read benchmarks against both a regional and a zonal bucket:
```bash
python gcsfs/tests/perf/microbenchmarks/run.py \
  --group read \
  --regional-bucket your-regional-bucket \
  --zonal-bucket your-zonal-bucket
```

The script will create a timestamped directory in `gcsfs/tests/perf/microbenchmarks/__run__/` containing the JSON and CSV results, and it will print a summary table to the console.
