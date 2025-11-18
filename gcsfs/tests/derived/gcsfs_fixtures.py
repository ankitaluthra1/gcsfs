import fsspec
import pytest
import os
from fsspec.tests.abstract import AbstractFixtures

from gcsfs.core import GCSFileSystem
from gcsfs.tests.conftest import allfiles
from gcsfs.tests.settings import TEST_BUCKET


class GcsfsFixtures(AbstractFixtures):
    @pytest.fixture(scope="class")
    def fs(self, docker_gcs):
        GCSFileSystem.clear_instance_cache()
        gcs = fsspec.filesystem("gcs", endpoint_url=docker_gcs)
        is_real_gcs = (
            os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
        )
        try:
            if not is_real_gcs:
            # ensure we're empty.
                try:
                    gcs.rm(TEST_BUCKET, recursive=True)
                except FileNotFoundError:
                    pass
                try:
                    gcs.mkdir(TEST_BUCKET)
                except Exception:
                    pass

            gcs.pipe({TEST_BUCKET + "/" + k: v for k, v in allfiles.items()})
            gcs.invalidate_cache()
            yield gcs
        finally:
            try:
                if not is_real_gcs:
                    gcs.rm(gcs.find(TEST_BUCKET))
                    gcs.rm(TEST_BUCKET)
            except:  # noqa: E722
                pass

    @pytest.fixture
    def fs_path(self):
        return TEST_BUCKET

    @pytest.fixture
    def supports_empty_directories(self):
        return False
