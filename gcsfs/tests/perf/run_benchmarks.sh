#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Default Benchmark Parameters ---
NUM_FILES=1
FILE_SIZE_MB=1
ROUNDS=10
ITERATIONS=1
CHUNK_SIZE_MB=16 # Default chunk size in MB
PROFILE_ENABLED=false
BUCKET=""   # Mandatory
PROJECT="" # Mandatory

function usage() {
    echo "Usage: $0 -b <bucket> -p <project> [-n num_files] [-s size_mb] [-r rounds] [-i iterations] [-c chunk_mb] [--profile]"
    echo "  -n: Number of files to create for the benchmark (default: $NUM_FILES)"
    echo "  -s: Size of each file in Megabytes (MB) (default: $FILE_SIZE_MB)"
    echo "  -r: Number of benchmark rounds (default: $ROUNDS)"
    echo "  -i: Number of iterations per round (default: $ITERATIONS)"
    echo "  -c: Chunk size for read/write operations in Megabytes (MB) (default: $CHUNK_SIZE_MB)"
    echo "  -b: GCS bucket to use for the benchmark (MANDATORY)"
    echo "  -p: GCP project to use (MANDATORY)"
    echo "  --profile: Enable cProfile for the benchmark run (default: disabled)"
    exit 1
}

while getopts "n:s:r:i:c:b:p:h" opt; do
  case "$opt" in
    n) NUM_FILES=$OPTARG ;;
    s) FILE_SIZE_MB=$OPTARG ;;
    r) ROUNDS=$OPTARG ;;
    i) ITERATIONS=$OPTARG ;;
    c) CHUNK_SIZE_MB=$OPTARG ;;
    b) BUCKET=$OPTARG ;;
    p) PROJECT=$OPTARG ;;
    h) usage ;;
    *) usage ;;
    -)
      case "${OPTARG}" in
        profile) PROFILE_ENABLED=true ;;
        *) usage ;;
      esac
      ;;
  esac
done

# Check for mandatory arguments
if [ -z "$BUCKET" ] || [ -z "$PROJECT" ]; then
    echo "Error: Bucket name (-b) and Project ID (-p) are mandatory."
    usage
fi

# Get the directory of the script to build absolute paths
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$SCRIPT_DIR/../../../.."
BENCHMARK_DIR="$SCRIPT_DIR/microbenchmarks"
JSON_OUTPUT="$PROJECT_ROOT/benchmark_results.json"
TSV_OUTPUT="${JSON_OUTPUT%.json}.tsv"

echo "--- GCSFS Benchmark Runner ---"

# 1. Delete benchmark_results.json file if present
echo "1. Cleaning up previous results..."
rm -f "$JSON_OUTPUT"

# Export parameters as environment variables for pytest
export GCSFS_BENCH_NUM_FILES=$NUM_FILES
export GCSFS_BENCH_FILE_SIZE_MB=$FILE_SIZE_MB
export GCSFS_BENCH_ROUNDS=$ROUNDS
export GCSFS_BENCH_ITERATIONS=$ITERATIONS
export GCSFS_BENCH_CHUNK_SIZE_MB=$CHUNK_SIZE_MB
export GCSFS_BENCH_BUCKET=$BUCKET
export GCSFS_BENCH_PROJECT=$PROJECT

# 2. Run all pytest benchmarks with JSON output
echo "2. Running pytest benchmarks with the following settings:"
echo "   - Num Files: $NUM_FILES, File Size: ${FILE_SIZE_MB}MB, Chunk Size: ${CHUNK_SIZE_MB}MB"
echo "   - Bucket: $BUCKET, Project: $PROJECT"
echo "   - Rounds: $ROUNDS, Iterations: $ITERATIONS"
echo "   - Profiling Enabled: $PROFILE_ENABLED"

PYTEST_ARGS=("$BENCHMARK_DIR" "--benchmark-json=$JSON_OUTPUT")
[ "$PROFILE_ENABLED" = true ] && PYTEST_ARGS+=("--benchmark-cprofile=tottime")

pytest "${PYTEST_ARGS[@]}"

echo "3. Processing benchmark results..."

# 3. Read the JSON and format it into a table using jq
# Check if jq is installed
if ! command -v jq &> /dev/null
then
    echo "jq not found. Attempting to install it automatically..."
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if command -v apt-get &> /dev/null; then
            sudo apt-get update && sudo apt-get install -y jq
        elif command -v yum &> /dev/null; then
            sudo yum install -y jq
        else
            echo "Error: Could not find apt-get or yum. Please install jq manually."
            exit 1
        fi
    else
        echo "Error: Unsupported OS '$OSTYPE'. Please install jq manually."
        exit 1
    fi

    if ! command -v jq &> /dev/null; then
        echo "jq installation failed. Please install it manually."
        exit 1
    fi
    echo "jq installed successfully."
fi

# Define the header for the output table
HEADER="Group\tNum_Files\tFile_Size(MB)\tChunk_Size(MB)\tMin(s)\tMax(s)\tMean(s)\tRounds\tIters\tP90(s)\tP95(s)\tP99(s)\tThroughput(MB/s)"

# Use jq to parse the JSON, calculate metrics, and format as TSV
jq -r '
  # Function to calculate percentile
  def percentile(p):
    ( (.stats.data | sort)[((.stats.data | length) * p / 100) | floor] ) as $p_val |
    ($p_val | tostring | .[0:8]);

  # Function to calculate throughput
  def throughput:
    if (.group | contains("LIST")) or .stats.mean == 0 then "N/A"
    else ((.extra_info.num_files * .extra_info.file_size) / (1024*1024) / .stats.mean) | tostring | .[0:8]
    end;

  # Main processing logic
  .benchmarks[] |
  [
    .group,
    .extra_info.num_files,
    (.extra_info.file_size / (1024*1024)),
    (if .extra_info.chunk_size then (.extra_info.chunk_size / (1024*1024)) else "N/A" end),
    (.stats.min | tostring | .[0:8]),
    (.stats.max | tostring | .[0:8]),
    (.stats.mean | tostring | .[0:8]),
    .stats.rounds,
    .stats.iterations,
    percentile(90),
    percentile(95),
    percentile(99),
    throughput
  ] | @tsv
' "$JSON_OUTPUT" | (echo -e "$HEADER" && cat) | column -t -s $'\t' > "$TSV_OUTPUT"

echo -e "\n--- Consolidated Benchmark Results ---"
cat "$TSV_OUTPUT"

echo -e "\n--- Benchmark run complete ---"
echo "Raw python-benchmark results are saved in: $JSON_OUTPUT"
echo "Formatted results are saved in: $TSV_OUTPUT"