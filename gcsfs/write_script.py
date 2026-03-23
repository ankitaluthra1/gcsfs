import time

import fsspec

# --- Upload Metrics for 40 GB file with existing reads---
# Total Upload Time: 2939.34 seconds (48.99 minutes)
# Write Throughput:  13.94 MB/s

# --- Upload Metrics for 40 B with updated write ---
# Total Upload Time: 1928.54 seconds (32.14 minutes)
# Write Throughput:  21.24 MB/s


def write_file_to_gcs_in_one_go(gcs_path: str):
    file_size_bytes = 40 * 1024**3

    print("Generating 40 GB of data in memory...")
    print(
        "Warning: Ensure your system has 80+ GB of RAM to survive this for standard buckets!"
    )

    # Generate the payload in memory (Not timed)
    in_memory_data = b"x" * file_size_bytes

    print(
        f"Successfully generated {len(in_memory_data) / (1024 ** 3):.2f} GB of data in RAM."
    )
    print(f"Starting upload to {gcs_path}...")

    # --- START TIMING THE UPLOAD ---
    start_time = time.perf_counter()

    with fsspec.open(gcs_path, "wb") as f:
        # Passing the full 20 GB file in a single write call
        f.write(in_memory_data)

    # --- STOP TIMING THE UPLOAD ---
    end_time = time.perf_counter()

    # Calculate time and throughput
    upload_time_seconds = end_time - start_time
    upload_time_minutes = upload_time_seconds / 60

    # Calculate throughput in MB/s (using 1024^2 for binary megabytes)
    file_size_mb = file_size_bytes / (1024**2)
    throughput_mbps = file_size_mb / upload_time_seconds

    print("\n--- Upload Metrics ---")
    print(
        f"Total Upload Time: {upload_time_seconds:.2f} seconds ({upload_time_minutes:.2f} minutes)"
    )
    print(f"Write Throughput:  {throughput_mbps:.2f} MB/s")


# --- Example Usage ---
if __name__ == "__main__":
    # Replace with your actual GCS bucket and desired file name
    target_destination = "gs://<your-bucket>/test_40gb_file_new.dat"
    write_file_to_gcs_in_one_go(target_destination)
