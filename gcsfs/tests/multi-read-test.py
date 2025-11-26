import fsspec
from concurrent.futures import ThreadPoolExecutor
import random
import os

os.environ["GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"]="true"
BUCKET = "<bucket-name>"
OBJECT = "<object-name>"
NUM_THREADS = 16
CHUNK_SIZE = 1024 * 1024

def random_seek_read(offset, size, fs, path):
    """Open the file, seek to offset, and read size bytes."""
    # Open in binary read mode
    with fs.open(path, "rb") as f:
        f.seek(offset)
        return f.read(size)

def main():
    print(os.environ.get("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"))
    fs = fsspec.filesystem(protocol="gs", project="<project-name>")
    path = f"{BUCKET}/{OBJECT}"

    # Get file size
    info = fs.info(path)
    size = info["size"]

    # Choose random offsets
    offsets = [
        random.randint(0, max(0, size - CHUNK_SIZE))
        for _ in range(NUM_THREADS)
    ]

    # Run parallel reads
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [
            executor.submit(random_seek_read, off, CHUNK_SIZE, fs, path)
            for off in offsets
        ]

    chunks = [f.result() for f in futures]
    print(f"Read {len(chunks)} chunks:")
    for i, c in enumerate(chunks):
        print(f"Chunk {i}: {c[:50]} ... (len={len(c)})")

if __name__ == "__main__":
    main()