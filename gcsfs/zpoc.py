import asyncio
from io import BytesIO

from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import AsyncMultiRangeDownloader

from .core import GCSFileSystem


async def main():
    test_project = "vaibhavpratap-sdk-test"
    object_path = "chandrasiri-rs/sunidhi"
    fs_zonal = GCSFileSystem(project = test_project, experimental_zb_hns_support=True)
    # ------------------------------
    # client = AsyncGrpcClient().grpc_client
    # mrd = await AsyncMultiRangeDownloader.create_mrd(
    #     client, bucket_name="chandrasiri-rs", object_name="sunidhi"
    # )
    # buff = BytesIO()
    # results_arr = await mrd.download_ranges([(1, 10, buff)])
    # for result in results_arr:
    #     print(result)
    # print(buff.getvalue())
    # ----------------------------------
    with fs_zonal.open(object_path, 'rb') as f:
        # > ZonalFileSystem._open() called for path: my-bucket/file.txt

        print("\nfile opened correctly")
        chunk = f.read(10)
        print("Chunk: ", chunk)
        f.write(b'data')

if __name__ == "__main__":
    asyncio.run(main())