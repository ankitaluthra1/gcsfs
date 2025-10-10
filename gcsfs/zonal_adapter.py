from fsspec import asyn
from google.cloud import storage
import re
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import (AsyncMultiRangeDownloader)
from io import BytesIO


def _parse_path(path):
    match = re.match(r"https://storage.googleapis.com/storage/v1/b/([^/]+)/o/([^?]+)", path)
    if match:
        return match.groups()
    return None, None


class ZonalAdapter:
    def __init__(self, gcs_file):
        self.gcs_file = gcs_file
        self.bucket = gcs_file.bucket
        self.object_path = gcs_file.key
        self.mrd = None

    @classmethod
    async def _create(cls, gcs_file):
        async_multi_range_reader = await AsyncMultiRangeDownloader.create_mrd(
            gcs_file.gcsfs.async_grpc_client,  gcs_file.bucket, gcs_file.key
        )
        return cls(gcs_file, async_multi_range_reader)
    create = asyn.sync_wrapper(_create)

    async def _async_mrd(self, path, start, end):
        print(f"\nAsync MRD called for path: {path} with start: {start} and end: {end}")
        if self.mrd is None:
            self.mrd=await AsyncMultiRangeDownloader.create_mrd(
                self.gcs_file.gcsfs.async_grpc_client,  self.bucket, self.object_path
        )
        print("\nUsing AsyncMultiRangeReader to fetch data.\n")

        buff = BytesIO()

        offset = start
        length = (end - start) + 1
        try:
            await self.mrd.download_ranges([(offset, length, buff)])

            print("\ndownloaded bytes: ", buff.getbuffer().nbytes)
            downloaded_data = buff.getvalue()
            print(downloaded_data)
            return downloaded_data
        except Exception as e:
            print("Exception in _async_mrd: ", e)

    sync_mrd=asyn.sync_wrapper(_async_mrd)

    def handle(self, path, start, end):
        try:
            return  asyn.sync(
        self.gcs_file.gcsfs.loop,      # The event loop to use
        self._async_mrd,  # The async function to run
          path, start, end                # The argument to pass to ZonalAdapter.create
    )
        except Exception as e:
            print("Exception raised in handling Zonal bucket request", e)
            raise
