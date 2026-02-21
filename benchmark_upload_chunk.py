
import time
import os
import sys
import argparse
import random
import string
import gc

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

def benchmark_upload(bucket_name, size_mb, use_new, cleanup=True):
    print(f"--- Benchmarking with use_new_upload_chunk={use_new} ---")
    
    # Toggle the flag in the module
    gcsfs.core.use_new_upload_chunk = use_new
    
    fs = GCSFileSystem()
    
    # Verify the flag is set correctly
    if gcsfs.core.use_new_upload_chunk != use_new:
        print(f"Warning: Flag was not set correctly! Expected {use_new}, got {gcsfs.core.use_new_upload_chunk}")
    
    # Create a unique filename
    filename = f"{bucket_name}/benchmark_{int(time.time())}_{'new' if use_new else 'old'}.bin"
    
    data = generate_data(size_mb)
    size_bytes = len(data)
    
    # Force GC before starting
    gc.collect()
    
    print(f"Starting upload to gs://{filename}...")
    start_time = time.time()
    
    # Use file.write() (buffered write) which exercises GCSFile._upload_chunk
    # Set block_size equal to total size to force a single huge chunk upload (exercising large buffer logic)
    # Or at least large enough to notice copy overhead (e.g. 100MB+)
    block_size = max(50 * 1024 * 1024, size_bytes) # At least 50MB, or full size
    
    print(f"Using block_size: {block_size / (1024*1024):.2f} MB")
    
    try:
        with fs.open(filename, 'wb', block_size=block_size) as f:
            f.write(data)
    except Exception as e:
        print(f"Upload failed: {e}")
        return None, None
    
    end_time = time.time()
    duration = end_time - start_time
    throughput = size_bytes / duration / (1024 * 1024) # MB/s
    
    print(f"Upload completed: {filename}")
    print(f"Time: {duration:.2f} seconds")
    print(f"Throughput: {throughput:.2f} MB/s")
    
    # Cleanup
    if cleanup:
        try:
            print(f"Cleaning up {filename}...")
            fs.rm(filename)
        except Exception as e:
            print(f"Cleanup failed: {e}")
    else:
        print(f"Skipping cleanup. File remains at gs://{filename}")
        
    return duration, throughput

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark GCSFS upload_chunk optimization")
    parser.add_argument("--bucket", help="GCS bucket name to use for benchmarking. If not provided, tries to list buckets.")
    parser.add_argument("--size", type=int, default=50, help="Size of data in MB (default: 50)")
    parser.add_argument("--no-cleanup", action="store_true", help="Do not delete the uploaded files after benchmark")
    args = parser.parse_args()
    
    bucket_name = args.bucket
    cleanup = not args.no_cleanup
    
    if not bucket_name:
        print("No bucket provided. Attempting to list available buckets...")
        try:
            fs = GCSFileSystem()
            buckets = fs.buckets
            if buckets:
                bucket_name = buckets[0]
                print(f"Using first available bucket: {bucket_name}")
            else:
                print("No buckets found. Please provide a bucket name with --bucket.")
                sys.exit(1)
        except Exception as e:
            print(f"Failed to list buckets: {e}")
            print("Please provide a bucket name with --bucket.")
            sys.exit(1)

    print(f"Benchmarking upload of {args.size} MB data to gs://{bucket_name}")
    
    # Run Old
    duration_old, throughput_old = benchmark_upload(bucket_name, args.size, use_new=False, cleanup=cleanup)
    
    if duration_old is None:
        print("Benchmark failed for old implementation.")
        sys.exit(1)

    # Run New
    duration_new, throughput_new = benchmark_upload(bucket_name, args.size, use_new=True, cleanup=cleanup)

    if duration_new is None:
        print("Benchmark failed for new implementation.")
        sys.exit(1)
    
    print("\n--- Results Comparison ---")
    print(f"Old Implementation: {throughput_old:.2f} MB/s")
    print(f"New Implementation: {throughput_new:.2f} MB/s")
    
    improvement = (throughput_new - throughput_old) / throughput_old * 100
    print(f"Improvement: {improvement:.2f}%")
