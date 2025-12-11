"""Tests for ZonalFile write operations."""

import os
from unittest import mock

import pytest

from gcsfs.tests.settings import TEST_ZONAL_BUCKET
from gcsfs.tests.utils import tempdir, tmpfile

file_path = f"{TEST_ZONAL_BUCKET}/zonal-file-test"
test_data = b"hello world"

REQUIRED_ENV_VAR = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"

# If the condition is True, only then tests in this file are run.
should_run = os.getenv(REQUIRED_ENV_VAR, "false").lower() in (
    "true",
    "1",
)
pytestmark = pytest.mark.skipif(
    not should_run, reason=f"Skipping tests: {REQUIRED_ENV_VAR} env variable is not set"
)


@pytest.mark.parametrize(
    "setup_action, error_match",
    [
        (lambda f: setattr(f, "mode", "rb"), "File not in write mode"),
        (lambda f: setattr(f, "closed", True), "I/O operation on closed file"),
        (
            lambda f: setattr(f, "forced", True),
            "This file has been force-flushed, can only close",
        ),
    ],
    ids=["not_writable", "closed", "force_flushed"],
)
def test_zonal_file_write_value_errors(
    extended_gcsfs, zonal_write_mocks, setup_action, error_match  # noqa: F841
):
    """Test ZonalFile.write raises ValueError for invalid states."""
    with extended_gcsfs.open(file_path, "wb") as f:
        setup_action(f)
        with pytest.raises(ValueError, match=error_match):
            f.write(test_data)


def test_zonal_file_write_success(extended_gcsfs, zonal_write_mocks):
    """Test that writing to a ZonalFile calls the underlying writer's append method."""
    with extended_gcsfs.open(file_path, "wb") as f:
        f.write(test_data)

    zonal_write_mocks["aaow"].append.assert_awaited_once_with(test_data)


def test_zonal_file_open_write_mode(extended_gcsfs, zonal_write_mocks):
    """Test that opening a ZonalFile in write mode initializes the writer."""
    bucket, key, _ = extended_gcsfs.split_path(file_path)
    with extended_gcsfs.open(file_path, "wb"):
        pass

    zonal_write_mocks["init_aaow"].assert_called_once_with(
        extended_gcsfs.grpc_client, bucket, key
    )


def test_zonal_file_flush(extended_gcsfs, zonal_write_mocks):
    """Test that flush calls the underlying writer's flush method."""
    with extended_gcsfs.open(file_path, "wb") as f:
        f.flush()

    zonal_write_mocks["aaow"].flush.assert_awaited()


def test_zonal_file_commit(extended_gcsfs, zonal_write_mocks):
    """Test that commit finalizes the write and sets autocommit to True."""
    with extended_gcsfs.open(file_path, "wb") as f:
        f.commit()

    zonal_write_mocks["aaow"].finalize.assert_awaited_once()
    assert f.autocommit is True


def test_zonal_file_discard(extended_gcsfs, zonal_write_mocks):  # noqa: F841
    """Test that discard on a ZonalFile logs a warning."""
    with mock.patch("gcsfs.zonal_file.logger") as mock_logger:
        with extended_gcsfs.open(file_path, "wb") as f:
            f.discard()
        mock_logger.warning.assert_called_once()
        assert (
            "Discard is not applicable for Zonal Buckets"
            in mock_logger.warning.call_args[0][0]
        )


def test_zonal_file_close(extended_gcsfs, zonal_write_mocks):
    """Test that close finalizes the write by default (autocommit=True)."""
    with extended_gcsfs.open(file_path, "wb"):
        pass
    zonal_write_mocks["aaow"].close.assert_awaited_once_with(finalize_on_close=True)


def test_zonal_file_close_with_autocommit_false(extended_gcsfs, zonal_write_mocks):
    """Test that close does not finalize the write when autocommit is False."""

    with extended_gcsfs.open(file_path, "wb", autocommit=False):
        pass  # close is called on exit

    zonal_write_mocks["aaow"].close.assert_awaited_once_with(finalize_on_close=False)


def test_zonal_file_not_implemented_method(extended_gcsfs, zonal_write_mocks):
    """Test that some GCSFile methods are not implemented for ZonalFile."""
    with extended_gcsfs.open(file_path, "wb") as f:
        method_to_call = getattr(f, "_upload_chunk")
        with pytest.raises(NotImplementedError):
            method_to_call()


@pytest.mark.skipif(
    os.environ.get("STORAGE_EMULATOR_HOST") != "https://storage.googleapis.com",
    reason="This test class is for real GCS only.",
)
class TestZonalFileRealGCS:
    """
    Contains tests for ZonalFile write operations that run only against a
    real GCS backend. These tests validate end-to-end write behavior.
    """

    def test_simple_upload_overwrite_behavior(self, extended_gcsfs):
        """Tests simple writes to a ZonalFile and verifies the content is overwritten"""
        with extended_gcsfs.open(file_path, "wb") as f:
            f.write(test_data)
        with extended_gcsfs.open(file_path, "wb", content_type="text/plain") as f:
            f.write(b"Sample text data.")
        assert extended_gcsfs.cat(file_path) == b"Sample text data."

    def test_large_upload(self, extended_gcsfs):
        """Tests writing a large chunk of data to a ZonalFile."""
        large_data = b"a" * (5 * 1024 * 1024)  # 5MB
        with extended_gcsfs.open(file_path, "wb") as f:
            f.write(large_data)
        assert extended_gcsfs.cat(file_path) == large_data

    def test_multiple_writes(self, extended_gcsfs):
        """Tests multiple write calls to the same ZonalFile handle."""
        data1 = b"first part "
        data2 = b"second part"
        with extended_gcsfs.open(file_path, "wb") as f:
            f.write(data1)
            f.write(data2)
        assert extended_gcsfs.cat(file_path) == data1 + data2

    @pytest.mark.skip(
        reason="Skipping put tests until append_from_file is implemented."
    )
    def test_put_file_to_zonal_bucket(self, extended_gcsfs):
        """Test putting a large file to a Zonal bucket."""
        remote_path = f"{TEST_ZONAL_BUCKET}/put_large_file"
        data = os.urandom(1 * 1024 * 1024)  # 1MB random data

        with tmpfile() as local_f:
            with open(local_f, "wb") as f:
                f.write(data)
            extended_gcsfs.put(local_f, remote_path)

        assert extended_gcsfs.exists(remote_path)
        assert extended_gcsfs.cat(remote_path) == data

    @pytest.mark.skip(
        reason="Skipping put tests until append_from_file is implemented."
    )
    def test_put_overwrite_in_zonal_bucket(self, extended_gcsfs):
        """Test that put overwrites an existing file in a Zonal bucket."""
        remote_path = f"{TEST_ZONAL_BUCKET}/put_overwrite"
        initial_data = b"initial data for put overwrite"
        overwrite_data = b"overwritten data for put"

        with tmpfile() as local_f:
            with open(local_f, "wb") as f:
                f.write(initial_data)
            extended_gcsfs.put(local_f, remote_path)

        assert extended_gcsfs.cat(remote_path) == initial_data

        with tmpfile() as local_f_overwrite:
            with open(local_f_overwrite, "wb") as f:
                f.write(overwrite_data)
            extended_gcsfs.put(local_f_overwrite, remote_path)

        assert extended_gcsfs.cat(remote_path) == overwrite_data

    @pytest.mark.skip(
        reason="Skipping put tests until append_from_file is implemented."
    )
    def test_put_directory_to_zonal_bucket(self, extended_gcsfs):
        """Test putting a directory recursively to a Zonal bucket."""
        remote_dir = f"{TEST_ZONAL_BUCKET}/zonal_put_dir"
        data1 = b"file one content"
        data2 = b"file two content"

        with tempdir() as local_dir:
            # Create a local directory structure
            os.makedirs(os.path.join(local_dir, "subdir"))
            with open(os.path.join(local_dir, "subdir", "file1.txt"), "wb") as f:
                f.write(data1)
            with open(os.path.join(local_dir, "subdir", "file2.txt"), "wb") as f:
                f.write(data2)

            # Upload the directory
            extended_gcsfs.put(
                os.path.join(local_dir, "subdir"), remote_dir, recursive=True
            )

        # Verify the upload
        assert extended_gcsfs.isdir(remote_dir)
        remote_files = extended_gcsfs.ls(remote_dir)
        assert len(remote_files) == 2
        assert f"{remote_dir}/file1.txt" in remote_files
        assert f"{remote_dir}/file2.txt" in remote_files

        assert extended_gcsfs.cat(f"{remote_dir}/file1.txt") == data1
        assert extended_gcsfs.cat(f"{remote_dir}/file2.txt") == data2

    def test_pipe_data_to_zonal_bucket(self, extended_gcsfs):
        """Test piping a small amount of data to a Zonal bucket."""
        remote_path = f"{TEST_ZONAL_BUCKET}/pipe_small"
        data = b"some small piped data"

        extended_gcsfs.pipe(remote_path, data)

        assert extended_gcsfs.exists(remote_path)
        assert extended_gcsfs.cat(remote_path) == data

    def test_pipe_overwrite_in_zonal_bucket(self, extended_gcsfs):
        """Test that pipe overwrites an existing file in a Zonal bucket."""
        remote_path = f"{TEST_ZONAL_BUCKET}/pipe_overwrite"
        initial_data = b"initial data for pipe overwrite"
        overwrite_data = b"overwritten piped data for pipe"

        extended_gcsfs.pipe(remote_path, initial_data)
        assert extended_gcsfs.cat(remote_path) == initial_data

        extended_gcsfs.pipe(remote_path, overwrite_data)
        assert extended_gcsfs.cat(remote_path) == overwrite_data
