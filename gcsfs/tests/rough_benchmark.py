import time
import os
import sys
import argparse
import random
import string

# Ensure current directory is in path to import modified gcsfs
sys.path.insert(0, os.path.abspath(os.getcwd()))

try:
  import gcsfs.core
  from gcsfs.core import GCSFileSystem, use_new_upload_chunk
except ImportError:
  print("Error: Could not import gcsfs from local directory.")
  sys.exit(1)


def generate_data(size_mb):
  """Generate random bytes of specified size in MB"""
  print(f"Generating {size_mb} MB of random data...")
  return os.urandom(size_mb * 1024 * 1024)


def benchmark_upload(bucket_name, size_mb, use_new):
  print(f"--- Benchmarking with use_new_upload_chunk={use_new} ---")

  # Toggle the flag in the module
  gcsfs.core.use_new_upload_chunk = use_new

  fs = GCSFileSystem()

  # Verify the flag is set correctly
  if gcsfs.core.use_new_upload_chunk != use_new:
    print(
      f"Warning: Flag was not set correctly! Expected {use_new}, got {gcsfs.core.use_new_upload_chunk}")

  # Create a unique filename
  filename = f"{bucket_name}/benchmark_{int(time.time())}_{'new' if use_new else 'old'}.bin"

  data = generate_data(size_mb)
  size_bytes = len(data)

  print(f"Starting upload to gs://{filename}...")
  start_time = time.time()

  # Use file.write() (buffered write) which exercises GCSFile._upload_chunk
  try:
    with fs.open(filename, 'wb') as f:
      f.write(data)
  except Exception as e:
    print(f"Upload failed: {e}")
    return None, None

  end_time = time.time()
  duration = end_time - start_time
  throughput = size_bytes / duration / (1024 * 1024)  # MB/s

  print(f"Upload completed: {filename}")
  print(f"Time: {duration:.2f} seconds")
  print(f"Throughput: {throughput:.2f} MB/s")

  # Cleanup
  # try:
  #   fs.rm(filename)
  # except Exception as e:
  #   print(f"Cleanup failed: {e}")

  return duration, throughput


def main():

  # Run Old
  duration_old, throughput_old = benchmark_upload("<bucket>", 1 * 1024,
                                                  use_new=False)

  if duration_old is None:
    print("Benchmark failed for old implementation.")
    return

  # Run New
  duration_new, throughput_new = benchmark_upload("<bucket>", 1 * 1024,
                                                  use_new=True)

  if duration_new is None:
    print("Benchmark failed for new implementation.")
    return

  print("\n--- Results Comparison ---")
  print(f"Old Implementation: {throughput_old:.2f} MB/s")
  print(f"New Implementation: {throughput_new:.2f} MB/s")

  improvement = (throughput_new - throughput_old) / throughput_old * 100
  print(f"Improvement: {improvement:.2f}%")


if __name__ == "__main__":
  main()
