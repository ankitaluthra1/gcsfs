#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Default Benchmark Parameters ---
NUM_FILES=1
FILE_SIZE_MB=1
ROUNDS=10
DEPTH=1
FILES_PER_DIR=1
ITERATIONS=1
CHUNK_SIZE_MB=16 # Default chunk size in MB
PROFILE_ENABLED=false
TEST_PATTERN="" # Pytest -k pattern
REGIONAL_BUCKET=""   # Mandatory
ZONAL_BUCKET=""
HNS_BUCKET=""
PROJECT="" # Mandatory
JSON_OUTPUT_PREFIX="benchmark_results"

function usage() {
    echo "Usage: $0 -p <project> [--regional-bucket <regional_bucket>] [--zonal-bucket <zonal_bucket>] [--hns-bucket <hns_bucket>] [--json-output-prefix <prefix>] [-n num_files] [-s size_mb] [-r rounds] [-i iterations] [-c chunk_mb] [-d depth] [-f files_per_dir] [-k test_pattern] [--profile]"
    echo "  -n: Number of files to create for the benchmark (default: $NUM_FILES)"
    echo "  -s: Size of each file in Megabytes (MB) (default: $FILE_SIZE_MB)"
    echo "  -r: Number of benchmark rounds (default: $ROUNDS)"
    echo "  -i: Number of iterations per round (default: $ITERATIONS)"
    echo "  -k: Pytest -k pattern to select tests (e.g. 'read or write') (default: all tests)"
    echo "  -d: For hierarchical benchmarks, the depth of the directory structure (default: $DEPTH)"
    echo "  -f: For hierarchical benchmarks, the number of files to create per directory (default: $FILES_PER_DIR)"
    echo "  -c: Chunk size for read/write operations in Megabytes (MB) (default: $CHUNK_SIZE_MB)"
    echo "  --json-output-prefix: Prefix for the output JSON file (default: benchmark_results)"
    echo "  --regional-bucket: (Optional) Regional GCS bucket to use for the benchmark."
    echo "  --zonal-bucket: (Optional) Zonal GCS bucket to run read benchmarks."
    echo "  --hns-bucket: (Optional) HNS-enabled GCS bucket to run all benchmarks."
    echo "  -p: GCP project to use (MANDATORY)"
    echo "  --profile: Enable cProfile for the benchmark run (default: disabled)"
    exit 1
}

check_dependencies() {
    echo "## Checking dependencies..."
    # Check for sysstat (provides pidstat)
    if ! command -v pidstat &> /dev/null; then
        echo "pidstat not found. Attempting to install sysstat..."
        if command -v apt-get &> /dev/null; then sudo apt-get update && sudo apt-get install -y sysstat;
        elif command -v yum &> /dev/null; then sudo yum install -y sysstat;
        elif command -v dnf &> /dev/null; then sudo dnf install -y sysstat;
        else echo "Could not find a supported package manager. Please install sysstat manually."; exit 1; fi
        if ! command -v pidstat &> /dev/null; then echo "sysstat installation failed."; exit 1; fi
    fi
    # Check for jq (json parsing)
    if ! command -v jq &> /dev/null; then
        echo "jq not found. Attempting to install..."
        if command -v apt-get &> /dev/null; then sudo apt-get update && sudo apt-get install -y jq;
        elif command -v yum &> /dev/null; then sudo yum install -y jq;
        elif command -v dnf &> /dev/null; then sudo dnf install -y jq;
        else echo "Could not find a supported package manager. Please install jq manually."; exit 1; fi
        if ! command -v jq &> /dev/null; then echo "jq installation failed."; exit 1; fi
    fi
    echo "Dependencies are satisfied."
}


parse_arguments() {
    while getopts ":n:s:r:i:c:p:k:d:f:h-:" opt; do
      case "$opt" in
        n) NUM_FILES=$OPTARG ;;
        s) FILE_SIZE_MB=$OPTARG ;;
        r) ROUNDS=$OPTARG ;;
        i) ITERATIONS=$OPTARG ;;
        d) DEPTH=$OPTARG ;;
        f) FILES_PER_DIR=$OPTARG ;;
        c) CHUNK_SIZE_MB=$OPTARG ;;
        p) PROJECT=$OPTARG ;;
        k) TEST_PATTERN=$OPTARG ;;
        h) usage ;;
        -)
          case "${OPTARG}" in
            regional-bucket) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); REGIONAL_BUCKET=$val ;;
            hns-bucket) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); HNS_BUCKET=$val ;;
            zonal-bucket) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); ZONAL_BUCKET=$val ;;
            json-output-prefix) val="${!OPTIND}"; OPTIND=$((OPTIND + 1)); JSON_OUTPUT_PREFIX=$val ;;
            profile) PROFILE_ENABLED=true ;;
            *) usage ;;
          esac
          ;;
        \?) echo "Invalid option: -$OPTARG" >&2; usage ;;
      esac
    done

    if [ -z "$PROJECT" ] || { [ -z "$REGIONAL_BUCKET" ] && [ -z "$ZONAL_BUCKET" ] && [ -z "$HNS_BUCKET" ]; }; then
        echo "Error: Project ID (-p) and at least one bucket are mandatory."
        usage
    fi
}

export_env_variables() {
    export GCSFS_BENCH_NUM_FILES=$NUM_FILES
    export GCSFS_BENCH_FILE_SIZE_MB=$FILE_SIZE_MB
    export GCSFS_BENCH_ROUNDS=$ROUNDS
    export GCSFS_BENCH_ITERATIONS=$ITERATIONS
    export GCSFS_BENCH_CHUNK_SIZE_MB=$CHUNK_SIZE_MB
    export GCSFS_BENCH_DEPTH=$DEPTH
    export GCSFS_BENCH_FILES_PER_DIR=$FILES_PER_DIR
    export GCSFS_BENCH_GROUP=$TEST_PATTERN
    export GCSFS_BENCH_PROJECT=$PROJECT
}

process_resource_usage() {
    local resource_log=$1
    local json_output_file=$2

    [ ! -f "$resource_log" ] && echo "Warning: Resource log file not found: $resource_log" && return
    [ ! -f "$json_output_file" ] && echo "Warning: Benchmark JSON file not found: $json_output_file" && return

    echo "Processing resource log: $resource_log"

    # Calculate aggregate stats using awk. CPU is in column 4 (%usr), Memory is in column 8 (RSS in KB).
    read -r avg_cpu max_cpu avg_mem max_mem < <(awk '
        /^#/ || !/^[0-9]/ {next}
        { cpu_sum+=$4; if($4>cpu_max) cpu_max=$4; mem_sum+=$8; if($8>mem_max) mem_max=$8; count++ }
        END {
            if (count > 0) {
                print cpu_sum/count, cpu_max, mem_sum/(count*1024), mem_max/1024
            } else {
                print 0, 0, 0, 0
            }
        }' "$resource_log")

    # Create a JSON object with the calculated stats
    local resource_json=$(jq -n \
        --arg acpu "$avg_cpu" --arg mcpu "$max_cpu" \
        --arg amem "$avg_mem" --arg mmem "$max_mem" \
        '{resource_usage: {cpu_usage: {average: $acpu, max: $mcpu}, memory_usage: {average_mb: $amem, max_mb: $mmem}}}')

    # Append the resource usage block to the main benchmark JSON file
    echo "Appending resource usage to $json_output_file"
    jq -s '.[0] * .[1]' "$json_output_file" <(echo "$resource_json") > "${json_output_file}.tmp" && mv "${json_output_file}.tmp" "$json_output_file"
}

run_pytest_with_monitoring() {
    local bucket_type=$1
    shift # Remove the bucket_type argument
    local pytest_args=("$@")

    # Define a log file for resource utilization stats based on bucket type
    local script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    local project_root="$script_dir/../../../.."
    local resource_log="$project_root/resource_stats_${JSON_OUTPUT_PREFIX}_${bucket_type}.log"

    # Extract the JSON output file path from the pytest arguments
    local json_output_file=""
    for arg in "${pytest_args[@]}"; do
        if [[ $arg == --benchmark-json=* ]]; then
            json_output_file="${arg#*=}"
        fi
    done
    echo "Executing: pytest ${pytest_args[*]}"

    # 1. Start pytest in the background
    pytest "${pytest_args[@]}" &
    local PYTEST_PID=$!

    echo "Monitoring resource utilization for pytest process Id: $PYTEST_PID"
    # 2. Start pidstat to monitor the pytest process
    # We add a check to ensure pidstat doesn't fail if the process is too short
    pidstat -p $PYTEST_PID -r -u 1 > "$resource_log" 2>/dev/null &
    local PIDSTAT_PID=$!

    # 3. Wait for pytest to finish
    wait $PYTEST_PID

    # 4. Stop the monitor
    kill $PIDSTAT_PID 2>/dev/null || true

    echo "Pytest running at process Id: $PYTEST_PID finished."

    # 5. Process the resource log and append results to the JSON file
    process_resource_usage "$resource_log" "$json_output_file"
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
    echo "Setting up 1 file for ${bucket_type} benchmark using gcloud..."
    echo "Updating gcloud components and enabling ${bucket_type} Bucket streaming..."
    gcloud components update --quiet
    gcloud config set storage/enable_zonal_buckets_bidi_streaming True

    echo "Creating a local ${FILE_SIZE_MB}MB file with random data..."
    dd if=/dev/urandom of=temp_file_for_upload bs=1M count=${FILE_SIZE_MB} iflag=fullblock status=none

    local zonal_file_name="zonal-file-$(uuidgen)"
    export GCSFS_BENCH_ZONAL_FILE_PATH="gs://$bucket_name/$zonal_file_name"
    gcloud storage cp temp_file_for_upload "$GCSFS_BENCH_ZONAL_FILE_PATH" --quiet
    rm "temp_file_for_upload"
    echo "Zonal setup complete."

    export GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT=true
    pytest_args+=("-k" "$current_test_pattern")

    run_pytest_with_monitoring "$bucket_type" "${pytest_args[@]}"
    unset GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT
    unset GCSFS_BENCH_ZONAL_FILE_PATH

    echo "Cleaning up zonal benchmark files..."
    gcloud storage rm "gs://$bucket_name/zonal-file-*" --quiet
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

    check_dependencies

    local script_dir=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
    local project_root="$script_dir/../../../.."
    local benchmark_dir="$script_dir/microbenchmarks"

    export_env_variables

    declare -A bucket_types
    [ -n "$REGIONAL_BUCKET" ] && bucket_types["regional"]=$REGIONAL_BUCKET
    [ -n "$HNS_BUCKET" ] && bucket_types["hns"]=$HNS_BUCKET
    [ -n "$ZONAL_BUCKET" ] && bucket_types["zonal"]=$ZONAL_BUCKET

    for bucket_type in "${!bucket_types[@]}"; do
        local bucket_name="${bucket_types[$bucket_type]}"
        local json_output="$project_root/${JSON_OUTPUT_PREFIX}_${bucket_type}.json"

        export GCSFS_BENCH_BUCKET=$bucket_name
        export GCSFS_BENCH_BUCKET_TYPE=$bucket_type

        local base_pytest_args=("$benchmark_dir" "--benchmark-json=$json_output")
        [ "$PROFILE_ENABLED" = true ] && base_pytest_args+=("--benchmark-cprofile=tottime")

        if [ "$bucket_type" == "zonal" ]; then
            run_zonal_benchmark "$bucket_type" "$bucket_name" "$TEST_PATTERN" "${base_pytest_args[@]}"
        else
            run_standard_benchmark "$bucket_type" "$bucket_name" "$TEST_PATTERN" "${base_pytest_args[@]}"
        fi
        unset GCSFS_BENCH_BUCKET_TYPE
    done

    echo -e "\n--- Benchmark run complete ---"
}

main "$@"
