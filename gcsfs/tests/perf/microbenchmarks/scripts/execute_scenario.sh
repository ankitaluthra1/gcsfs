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
TEST_PATTERN="" # Pytest -k pattern
PATTERN="seq" # Default read pattern
THREADS=1 # Default number of threads
BUCKET_NAME="" # Mandatory
BUCKET_TYPE="" # Mandatory

PROJECT="" # Mandatory
JSON_OUTPUT_PREFIX="benchmark_results"

function usage() {
    echo "Usage: $0 -p <project> [--regional-bucket <regional_bucket>] [--zonal-bucket <zonal_bucket>] [--hns-bucket <hns_bucket>] [--json-output-prefix <prefix>] [-n num_files] [-s size_mb] [-r rounds] [-i iterations] [-c chunk_mb] [-d depth] [-k test_pattern] [--pattern <pattern>] [--threads <threads>] [--profile]"
    echo "  -n: Number of files for the benchmark (default: $NUM_FILES)"
    echo "  -s: Size of each file in MB (default: $FILE_SIZE_MB)"
    echo "  -r: Number of benchmark rounds (default: $ROUNDS)"
    echo "  -k: Pytest -k pattern to select tests (e.g. 'read or write') (default: all tests)"
    echo "  -c: Chunk size for read/write operations in Megabytes (MB) (default: $CHUNK_SIZE_MB)"
    echo "  --pattern: Read pattern, 'seq' (sequential) or 'rand' (random) (default: $PATTERN)"
    echo "  --threads: Number of threads for read benchmarks (default: $THREADS)"
    echo "  --bucket-name: GCS bucket to use for the benchmark (MANDATORY)"
    echo "  --bucket-type: Type of the bucket ('regional', 'hns', 'zonal') (MANDATORY)"
    echo "  --json-output-prefix: Prefix for the output JSON file (default: benchmark_results)"
    echo "  -p: GCP project to use (MANDATORY)"
    echo "  --profile: Enable cProfile for the benchmark run (default: disabled)"
    exit 1
}


parse_arguments() {
    while getopts ":n:s:r:i:c:p:k:d:h-:" opt; do
      case "$opt" in
        n) NUM_FILES=$OPTARG ;;
        s) FILE_SIZE_MB=$OPTARG ;;
        r) ROUNDS=$OPTARG ;;
        c) CHUNK_SIZE_MB=$OPTARG ;;
        p) PROJECT=$OPTARG ;;
        k) TEST_PATTERN=$OPTARG ;;
        h) usage ;;
        -)
          case "${OPTARG}" in
            bucket-name) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); BUCKET_NAME=$val ;;
            bucket-type) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); BUCKET_TYPE=$val ;;
            json-output-prefix) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); JSON_OUTPUT_PREFIX=$val ;;
            threads) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); THREADS=$val ;;
            pattern) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); PATTERN=$val ;;
            profile) PROFILE_ENABLED=true ;;
            *) usage ;;
          esac
          ;;
        \?) echo "Invalid option: -$OPTARG" >&2; usage ;;
      esac
    done

    if [ -z "$PROJECT" ] || [ -z "$BUCKET_NAME" ] || [ -z "$BUCKET_TYPE" ]; then
        echo "Error: Project ID (-p), bucket name (--bucket-name), and bucket type (--bucket-type) are mandatory."
        usage
    fi
}

export_env_variables() {
    export GCSFS_BENCH_NUM_FILES=$NUM_FILES
    export GCSFS_BENCH_FILE_SIZE_MB=$FILE_SIZE_MB
    export GCSFS_BENCH_ROUNDS=$ROUNDS
    export GCSFS_BENCH_CHUNK_SIZE_MB=$CHUNK_SIZE_MB
    export GCSFS_BENCH_GROUP=$TEST_PATTERN
    export GCSFS_BENCH_PROJECT=$PROJECT
    export GCSFS_BENCH_PATTERN=$PATTERN
    export GCSFS_BENCH_THREADS=$THREADS
    export GCSFS_BENCH_BUCKET=$BUCKET_NAME
    export GCSFS_BENCH_BUCKET_TYPE=$BUCKET_TYPE
}

run_pytest_with_monitoring() {
    local bucket_type=$1
    shift # Remove the bucket_type argument
    local pytest_args=("$@")

    echo "Executing: pytest ${pytest_args[*]}"
    pytest "${pytest_args[@]}"
}

run_zonal_benchmark() {
    local bucket_type=$1
    local bucket_name=$2
    local test_pattern=$3
    local pytest_args=("${@:4}")

    if [[ -n "$test_pattern" && "$test_pattern" != *"read"* ]]; then
        echo "Skipping zonal benchmarks as test pattern '$test_pattern' does not include 'read'."
        return
    fi
    ## Only run read tests for now, this should be removed once we have write for ZB in place
    local current_test_pattern="read"

    echo -e "\n--- Running benchmarks for ${bucket_type} bucket: ${bucket_name} (Pattern: '${current_test_pattern}') ---"

    local zonal_file_prefix="zonal-file-bench"
    export GCSFS_BENCH_ZONAL_FILE_PATH="gs://${bucket_name}/${zonal_file_prefix}"

    echo "Zonal setup complete."

    pytest_args+=("-k" "$current_test_pattern")

    run_pytest_with_monitoring "$bucket_type" "${pytest_args[@]}"
    unset GCSFS_BENCH_ZONAL_FILE_PATH
}

run_standard_benchmark() {
    local bucket_type=$1
    local bucket_name=$2
    local test_pattern=$3
    local pytest_args=("${@:4}")

    local current_test_pattern=$test_pattern
    if [ -z "$current_test_pattern" ]; then
        current_test_pattern="read or write"
    fi

    echo -e "\n--- Running benchmarks for ${bucket_type} bucket: ${bucket_name} (Pattern: '${current_test_pattern}') ---"
    pytest_args+=("-k" "$current_test_pattern")

    run_pytest_with_monitoring "$bucket_type" "${pytest_args[@]}"
}

main() {
    parse_arguments "$@"

    local script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
    local project_root="$script_dir/../../../../.."
    local benchmark_dir="$script_dir/.."

    export_env_variables

    local json_output="$project_root/${JSON_OUTPUT_PREFIX}_${BUCKET_TYPE}.json"

    local base_pytest_args=("$benchmark_dir" "--benchmark-json=$json_output")
    [ "$PROFILE_ENABLED" = true ] && base_pytest_args+=("--benchmark-cprofile=tottime")

    if [ "$BUCKET_TYPE" == "zonal" ]; then
        run_zonal_benchmark "$BUCKET_TYPE" "$BUCKET_NAME" "$TEST_PATTERN" "${base_pytest_args[@]}"
    else
        run_standard_benchmark "$BUCKET_TYPE" "$BUCKET_NAME" "$TEST_PATTERN" "${base_pytest_args[@]}"
    fi

    echo -e "\n--- Benchmark run complete ---"
}

main "$@"
