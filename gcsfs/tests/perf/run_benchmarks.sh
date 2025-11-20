#!/bin/bash

set -e

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="$SCRIPT_DIR/../../../.."
CONFIG_FILE="$SCRIPT_DIR/benchmark_config.yaml" # Assumes yq v3 syntax
TSV_OUTPUT="$PROJECT_ROOT/consolidated_benchmark_results.tsv"
SCENARIO_NAME="" # Optional: a specific scenario name to run

function usage() {
    echo "Usage: $0 [-s <scenario_name>]"
    echo "  -s: (Optional) The specific benchmark scenario name from benchmark_config.yaml to run."
    exit 1
}

check_dependencies() {
    echo "## Checking dependencies..."
    if ! command -v yq &> /dev/null; then
        echo "yq not found. Attempting to install..."
        if command -v apt-get &> /dev/null; then sudo apt-get update && sudo apt-get install -y yq;
        elif command -v yum &> /dev/null; then sudo yum install -y yq;
        elif command -v dnf &> /dev/null; then sudo dnf install -y yq;
        else echo "Could not find a supported package manager. Please install yq (v3) manually."; exit 1; fi
        if ! command -v yq &> /dev/null; then echo "yq installation failed."; exit 1; fi
    fi
    echo "Dependencies are satisfied."
}

cleanup_previous_results() {
    echo "## Cleaning up previous results..."
    rm -f "$PROJECT_ROOT"/benchmark_results_*.json "$PROJECT_ROOT"/resource_stats_*.log "$TSV_OUTPUT"
}

load_common_config() {
    echo "## Loading Common Configuration..."
    export PROJECT=$(yq -r '.common.project' "$CONFIG_FILE")
    export PROFILE=$(yq -r '.common.profile' "$CONFIG_FILE")
    export REGIONAL_BUCKET=$(yq -r '.common.buckets.regional' "$CONFIG_FILE")
    export HNS_BUCKET=$(yq -r '.common.buckets.hns' "$CONFIG_FILE")
    export ZONAL_BUCKET=$(yq -r '.common.buckets.zonal' "$CONFIG_FILE")

    if [ "$PROJECT" = "null" ] || [ -z "$PROJECT" ]; then
        echo "Error: 'project' not defined in common config in $CONFIG_FILE"
        exit 1
    fi

    echo "Project: $PROJECT"
    [ "$REGIONAL_BUCKET" != "null" ] && echo "Regional Bucket: $REGIONAL_BUCKET"
    [ "$HNS_BUCKET" != "null" ] && echo "HNS Bucket: $HNS_BUCKET"
    [ "$ZONAL_BUCKET" != "null" ] && echo "Zonal Bucket: $ZONAL_BUCKET"
}

run_scenario() {
    local name=$1
    echo -e "\n## Starting scenario: $name"

    # Fetch the full YAML object for the given scenario name
    local scenario_yaml=$(yq ".benchmarks[] | select(.name == \"$name\")" "$CONFIG_FILE")
    if [[ -z "$scenario_yaml" || "$scenario_yaml" == "null" ]]; then
        echo "Warning: Scenario '$name' not found in $CONFIG_FILE. Skipping."
        return
    fi

    local group=$(echo "$scenario_yaml" | yq -r '.group')
    local num_files=$(echo "$scenario_yaml" | yq -r '.params.num_files')
    local file_size=$(echo "$scenario_yaml" | yq -r '.params.file_size')
    local chunk_size=$(echo "$scenario_yaml" | yq -r '.params.chunk_size')
    local depth=$(echo "$scenario_yaml" | yq -r '.params.depth')
    local files_per_dir=$(echo "$scenario_yaml" | yq -r '.params.files_per_dir')
    local rounds=$(echo "$scenario_yaml" | yq -r '.params.rounds')

    # Build the command for the worker script
    local CMD=("$SCRIPT_DIR/execute_scenario.sh" -p "$PROJECT" -k "$group")

    [ "$REGIONAL_BUCKET" != "null" ] && CMD+=(--regional-bucket "$REGIONAL_BUCKET")
    [ "$HNS_BUCKET" != "null" ] && CMD+=(--hns-bucket "$HNS_BUCKET")
    [ "$ZONAL_BUCKET" != "null" ] && CMD+=(--zonal-bucket "$ZONAL_BUCKET")

    [ "$num_files" != "null" ] && CMD+=(-n "$num_files")
    [ "$file_size" != "null" ] && CMD+=(-s "$file_size")
    [ "$chunk_size" != "null" ] && CMD+=(-c "$chunk_size")
    [ "$depth" != "null" ] && CMD+=(-d "$depth")
    [ "$files_per_dir" != "null" ] && CMD+=(-f "$files_per_dir")
    [ "$rounds" != "null" ] && CMD+=(-r "$rounds")
    [ "$PROFILE" = "yes" ] && CMD+=(--profile)

    CMD+=(--json-output-prefix "benchmark_results_${name//\"/}")

    # Execute the worker script
    "${CMD[@]}"
}

process_results() {
    echo -e "\n## All scenarios complete. Consolidating results..."
    local HEADER="Group\tBucket_Name\tBucket_Type\tNum_Files\tFile_Size(MB)\tChunk_Size(MB)\tMin(s)\tMax(s)\tMean(s)\tRounds\tIters\tP90(s)\tP95(s)\tP99(s)\tThroughput(MB/s)"

    # Check if any result files were created
    if ! ls "$PROJECT_ROOT"/benchmark_results_*.json 1> /dev/null 2>&1; then
        echo "No benchmark result files found. Skipping result processing."
        return
    fi

    # Use jq to parse all generated JSON files, calculate metrics, and format as TSV
    jq -r '
  # Function to calculate percentile
  def percentile(p):
    .stats.data | sort | .[((length * p / 100 + 0.5) | floor) - 1] | tostring | .[0:8];

  # Function to calculate throughput
  def throughput:
    if (.group | contains("LIST")) or .stats.mean == 0 then "N/A"
    else ((.extra_info.num_files * .extra_info.file_size) / (1024*1024) / .stats.mean) | tostring | .[0:8]
    end;

  # Main processing logic
  # Input is a stream of {bucket_type, data} objects
  .data.benchmarks[] |
  [
    .group,
    .extra_info.bucket_name,
    .extra_info.bucket_type,
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
' <(
      # Find all JSON files and wrap them in an object with their bucket type
      for f in "$PROJECT_ROOT"/benchmark_results_*.json; do
        local bucket_type=$(basename "$f" | sed -e 's/.*_\(regional\|hns\|zonal\)\.json/\1/')
        jq -c --arg bucket_type "$bucket_type" '{bucket_type: $bucket_type, data: .}' "$f"
      done
    ) | (echo -e "$HEADER" && cat) | column -t -s $'\t' > "$TSV_OUTPUT"

    echo -e "\n--- Consolidated Benchmark Results ---"
    cat "$TSV_OUTPUT"
    echo -e "\nFormatted results are saved in: $TSV_OUTPUT"
}

main() {
    [ "$1" = "-s" ] && [ -n "$2" ] && SCENARIO_NAME=$2

    echo "--- GCSFS Benchmark Orchestrator ---"
    
    check_dependencies
    cleanup_previous_results
    load_common_config

    # Get all available benchmark names from the config file
    local all_scenario_names=$(yq -r '.benchmarks[].name' "$CONFIG_FILE")
    
    if [ -z "$SCENARIO_NAME" ]; then
        echo -e "\n## No specific scenario requested. Running all benchmarks..."
        for name in $all_scenario_names; do
            run_scenario "$name"
        done
    else
        echo -e "\n## Running single scenario: $SCENARIO_NAME"
        # Validate that the requested scenario exists
        if ! echo "$all_scenario_names" | grep -q -w "$SCENARIO_NAME"; then
            echo "Error: Scenario '$SCENARIO_NAME' not found in $CONFIG_FILE"
            exit 1
        fi
        run_scenario "$SCENARIO_NAME" 
    fi

    process_results
    echo -e "\n--- Benchmark run complete ---"
}

main "$@"