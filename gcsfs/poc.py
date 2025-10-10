import asyncio
import io
import time
import logging
from enum import Enum, auto
from .core import GCSFileSystem

async def main():
    print("-" * 60)
    print("Starting GCSFS Asynchronous Multi-Range Read POC")
    print("-" * 60)

    fs = GCSFileSystem(token="google_default", experimental_zb_hns_support=True)
    print("The filesystem is created")
    # obj = fs.ls("chandrasiri-rs")
    # for obj in obj:
    #     print(obj)
    gcs_path = "chandrasiri-rs/sunidhi"  # Replace with your GCS file path
    f = fs.open(gcs_path, mode='rb')
    print("Opened file", gcs_path)
    # f.seek(1)
    # chunk = f.read(10)
    # print("Chunk: ", chunk)



if __name__ == "__main__":
    asyncio.run(main())
