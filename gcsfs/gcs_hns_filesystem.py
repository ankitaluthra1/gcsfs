import logging
from enum import Enum

from google.api_core import exceptions
from fsspec import asyn
from google.cloud.storage._experimental.asyncio.async_grpc_client import AsyncGrpcClient
from google.cloud import storage_control_v2

from .core import GCSFileSystem, GCSFile
from .zonal_file import ZonalFile

logger = logging.getLogger("gcsfs")


class BucketType(Enum):
    ZONAL_HIERARCHICAL = "ZONAL_HIERARCHICAL"
    HIERARCHICAL = "HIERARCHICAL"
    NON_HIERARCHICAL = "NON_HIERARCHICAL"
    UNKNOWN = "UNKNOWN"


gcs_file_types = {
    BucketType.ZONAL_HIERARCHICAL: ZonalFile,
    BucketType.HIERARCHICAL: GCSFile,
    BucketType.NON_HIERARCHICAL: GCSFile,
    BucketType.UNKNOWN: GCSFile,
    None: GCSFile,
}


class GCSHNSFileSystem(GCSFileSystem):
    """
    An subclass of GCSFileSystem that will contain specialized
    logic for HNS Filesystem.
    """

    def __init__(self, *args, **kwargs):
        kwargs.pop('experimental_zb_hns_support', None)
        super().__init__(*args, **kwargs)
        self.grpc_client = None
        self.grpc_client = asyn.sync(self.loop, self._create_grpc_client)
        self.control_plane_client = asyn.sync(self.loop, self._create_control_plane_client)
        self._storage_layout_cache = {}

    async def _create_grpc_client(self):
        if self.grpc_client is None:
            return AsyncGrpcClient().grpc_client
        else:
            return self.grpc_client

    async def _create_control_plane_client(self):
        # Initialize the client in the same async context to ensure it uses
        # the correct event loop.
        return storage_control_v2.StorageControlAsyncClient()

    async def _get_storage_layout(self, bucket):
        if bucket in self._storage_layout_cache:
            return self._storage_layout_cache[bucket]
        try:
            response = await self._call("GET", f"b/{bucket}/storageLayout", json_out=True)
            if response.get("locationType") == "zone":
                bucket_type = BucketType.ZONAL_HIERARCHICAL
            elif response.get("hierarchicalNamespace", {}).get("enabled"):
                bucket_type = BucketType.HIERARCHICAL
            else:
                bucket_type = BucketType.NON_HIERARCHICAL
            self._storage_layout_cache[bucket] = bucket_type
            return bucket_type
        except Exception as e:
            logger.error(f"Could not determine storage layout for bucket {bucket}: {e}")
            # Default to UNKNOWN
            self._storage_layout_cache[bucket] = BucketType.UNKNOWN
            return BucketType.UNKNOWN

    _sync_get_storage_layout = asyn.sync_wrapper(_get_storage_layout)

    def _open(
            self,
            path,
            mode="rb",
            **kwargs,
    ):
        """
        Open a file.
        """
        bucket, _, _ = self.split_path(path)
        bucket_type = self._sync_get_storage_layout(bucket)
        return gcs_file_types[bucket_type](gcsfs=self, path=path, mode=mode, **kwargs)

    def _process_limits(self, start, end):
        # Dummy method to process start and end
        if start is None:
            start = 0
        if end is None:
            end = 100
        return start, end - start + 1

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        """
        Fetch a file's contents as bytes.
        """
        mrd = kwargs.pop("mrd", None)
        if mrd is None:
            bucket, object_name, generation = self.split_path(path)
            mrd = await ZonalFile._create_mrd(self.grpc_client, bucket, object_name, generation)

        offset, length = self._process_limits(start, end)
        return await ZonalFile.download_range(offset=offset, length=length, mrd=mrd)
    
    async def _mkdir(self, path, create_parents=True, **kwargs):
        path = self._strip_protocol(path)
        bucket, key, _ = self.split_path(path)

        if not key:  # This is a bucket
            return await super()._mkdir(path, create_parents=create_parents, **kwargs)

        bucket_type = await self._get_storage_layout(bucket)

        if bucket_type in [BucketType.ZONAL_HIERARCHICAL, BucketType.HIERARCHICAL]:
            # For HNS buckets, use the control plane client to create a folder.
            exists = await self._exists(path)
            if not exists:
                parent = f"projects/_/buckets/{bucket}"
                folder_id = key.rstrip('/')
                request = storage_control_v2.CreateFolderRequest(
                    parent=parent,
                    folder_id=folder_id,
                    recursive=create_parents,
                )
                try:
                    await self.control_plane_client.create_folder(request=request)
                    self.invalidate_cache(self._parent(path))
                except exceptions.FailedPrecondition as e:
                    # This error occurs if create_parents=False and the parent dir doesn't exist.
                    # We translate it to FileNotFoundError for fsspec compatibility.
                    logger.warning(f"HNS _mkdir: FailedPrecondition for '{path}', likely missing parent. Re-raising as FileNotFoundError. Original error: {e}")
                    raise FileNotFoundError(f"Parent directory of {path} does not exist and create_parents is False.") from e
                except Exception as e: # Catch other potential exceptions
                    logger.error(f"HNS _mkdir: Failed to create folder '{path}': {e}")
                    raise
        else:
            return await super()._mkdir(path, create_parents=create_parents, **kwargs)

    mkdir = asyn.sync_wrapper(_mkdir)