import contextlib
import io
import os
from itertools import chain
from unittest import mock

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud.storage._experimental.asyncio.async_multi_range_downloader import (
    AsyncMultiRangeDownloader,
)
from google.cloud.storage.exceptions import DataCorruption

from gcsfs.checkers import ConsistencyChecker, MD5Checker, SizeChecker
from gcsfs.extended_gcsfs import BucketType
from gcsfs.tests.conftest import csv_files, files, text_files
from gcsfs.tests.settings import TEST_HNS_BUCKET, TEST_ZONAL_BUCKET

file = "test/accounts.1.json"
file_path = f"{TEST_ZONAL_BUCKET}/{file}"
json_data = files[file]
lines = io.BytesIO(json_data).readlines()
file_size = len(json_data)

REQUIRED_ENV_VAR = "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT"

a = TEST_ZONAL_BUCKET + "/tmp/test/a"
b = TEST_ZONAL_BUCKET + "/tmp/test/b"
c = TEST_ZONAL_BUCKET + "/tmp/test/c"

# If the condition is True, only then tests in this file are run.
should_run = os.getenv(REQUIRED_ENV_VAR, "false").lower() in (
    "true",
    "1",
)
pytestmark = pytest.mark.skipif(
    not should_run, reason=f"Skipping tests: {REQUIRED_ENV_VAR} env variable is not set"
)


@pytest.fixture
def gcs_bucket_mocks():
    """A factory fixture for mocking bucket functionality for different bucket types."""

    @contextlib.contextmanager
    def _gcs_bucket_mocks_factory(file_data, bucket_type_val):
        """Creates mocks for a given file content and bucket type."""
        is_real_gcs = (
            os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
        )
        if is_real_gcs:
            yield None
            return
        patch_target_lookup_bucket_type = (
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._lookup_bucket_type"
        )
        patch_target_sync_lookup_bucket_type = (
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._sync_lookup_bucket_type"
        )
        patch_target_create_mrd = (
            "google.cloud.storage._experimental.asyncio.async_multi_range_downloader"
            ".AsyncMultiRangeDownloader.create_mrd"
        )
        patch_target_gcsfs_cat_file = "gcsfs.core.GCSFileSystem._cat_file"

        async def download_side_effect(read_requests, **kwargs):
            if read_requests and len(read_requests) == 1:
                param_offset, param_length, buffer_arg = read_requests[0]
                if hasattr(buffer_arg, "write"):
                    buffer_arg.write(
                        file_data[param_offset : param_offset + param_length]
                    )
            return [mock.Mock(error=None)]

        mock_downloader = mock.Mock(spec=AsyncMultiRangeDownloader)
        mock_downloader.download_ranges = mock.AsyncMock(
            side_effect=download_side_effect
        )

        mock_create_mrd = mock.AsyncMock(return_value=mock_downloader)
        with (
            mock.patch(
                patch_target_sync_lookup_bucket_type, return_value=bucket_type_val
            ) as mock_sync_lookup_bucket_type,
            mock.patch(
                patch_target_lookup_bucket_type,
                return_value=bucket_type_val,
            ),
            mock.patch(patch_target_create_mrd, mock_create_mrd),
            mock.patch(
                patch_target_gcsfs_cat_file, new_callable=mock.AsyncMock
            ) as mock_cat_file,
        ):
            mocks = {
                "sync_lookup_bucket_type": mock_sync_lookup_bucket_type,
                "create_mrd": mock_create_mrd,
                "downloader": mock_downloader,
                "cat_file": mock_cat_file,
            }
            yield mocks
            # Common assertion for all tests using this mock
            mock_cat_file.assert_not_called()

    return _gcs_bucket_mocks_factory


@pytest.fixture
def gcs_hns_mocks():
    """A factory fixture for mocking bucket functionality for HNS mv tests."""

    @contextlib.contextmanager
    def _gcs_hns_mocks_factory(bucket_type_val, gcsfs):
        """Creates mocks for a given file content and bucket type."""
        is_real_gcs = (
            os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
        )
        if is_real_gcs:
            yield None
            return

        patch_target_lookup_bucket_type = (
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._lookup_bucket_type"
        )
        patch_target_sync_lookup_bucket_type = (
            "gcsfs.extended_gcsfs.ExtendedGcsFileSystem._sync_lookup_bucket_type"
        )
        patch_target_super_mv = "gcsfs.core.GCSFileSystem.mv"

        # Mock the async rename_folder method on the storage_control_client
        mock_rename_folder = mock.AsyncMock()
        mock_control_client_instance = mock.AsyncMock()
        mock_control_client_instance.rename_folder = mock_rename_folder

        with (
            mock.patch(
                patch_target_lookup_bucket_type, new_callable=mock.AsyncMock
            ) as mock_async_lookup_bucket_type,
            mock.patch(
                patch_target_sync_lookup_bucket_type
            ) as mock_sync_lookup_bucket_type,
            mock.patch(
                "gcsfs.core.GCSFileSystem._info", new_callable=mock.AsyncMock
            ) as mock_info,
            mock.patch.object(
                gcsfs, "_storage_control_client", mock_control_client_instance
            ),
            mock.patch(patch_target_super_mv, new_callable=mock.Mock) as mock_super_mv,
            mock.patch.object(
                gcsfs, "invalidate_cache", wraps=gcsfs.invalidate_cache
            ) as mock_invalidate_cache,
        ):
            mock_async_lookup_bucket_type.return_value = bucket_type_val
            mock_sync_lookup_bucket_type.return_value = bucket_type_val
            mocks = {
                "async_lookup_bucket_type": mock_async_lookup_bucket_type,
                "sync_lookup_bucket_type": mock_sync_lookup_bucket_type,
                "info": mock_info,
                "control_client": mock_control_client_instance,
                "super_mv": mock_super_mv,
                "invalidate_cache": mock_invalidate_cache,
            }
            yield mocks

    return _gcs_hns_mocks_factory


read_block_params = [
    # Read specific chunk
    pytest.param(3, 10, None, json_data[3 : 3 + 10], id="offset=3, length=10"),
    # Read from beginning up to length
    pytest.param(0, 5, None, json_data[0:5], id="offset=0, length=5"),
    # Read from offset to end (simulate large length)
    pytest.param(15, 5000, None, json_data[15:], id="offset=15, length=large"),
    # Read beyond end of file (should return empty bytes)
    pytest.param(file_size + 10, 5, None, b"", id="offset>size, length=5"),
    # Read exactly at the end (zero length)
    pytest.param(file_size, 10, None, b"", id="offset=size, length=10"),
    # Read with delimiter
    pytest.param(1, 35, b"\n", lines[1], id="offset=1, length=35, delimiter=newline"),
    pytest.param(0, 30, b"\n", lines[0], id="offset=0, length=35, delimiter=newline"),
    pytest.param(
        0, 35, b"\n", lines[0] + lines[1], id="offset=0, length=35, delimiter=newline"
    ),
]


def test_read_block_zb(extended_gcsfs, gcs_bucket_mocks, subtests):
    for param in read_block_params:
        with subtests.test(id=param.id):
            offset, length, delimiter, expected_data = param.values
            path = file_path

            with gcs_bucket_mocks(
                json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
            ) as mocks:
                result = extended_gcsfs.read_block(path, offset, length, delimiter)

                assert result == expected_data
                if mocks:
                    mocks["sync_lookup_bucket_type"].assert_called_once_with(
                        TEST_ZONAL_BUCKET
                    )
                    if expected_data:
                        mocks["downloader"].download_ranges.assert_called_with(
                            [(offset, mock.ANY, mock.ANY)]
                        )
                    else:
                        mocks["downloader"].download_ranges.assert_not_called()


@pytest.mark.parametrize("bucket_type_val", list(BucketType))
def test_open_uses_correct_blocksize_and_consistency_for_all_bucket_types(
    extended_gcs_factory, gcs_bucket_mocks, bucket_type_val
):
    csv_file = "2014-01-01.csv"
    csv_file_path = f"{TEST_ZONAL_BUCKET}/{csv_file}"
    csv_data = csv_files[csv_file]

    custom_filesystem_block_size = 100 * 1024 * 1024
    extended_gcsfs = extended_gcs_factory(
        block_size=custom_filesystem_block_size, consistency="md5"
    )

    with gcs_bucket_mocks(csv_data, bucket_type_val=bucket_type_val):
        with extended_gcsfs.open(csv_file_path, "rb") as f:
            assert f.blocksize == custom_filesystem_block_size
            assert isinstance(f.checker, MD5Checker)

        file_block_size = 1024 * 1024
        with extended_gcsfs.open(
            csv_file_path, "rb", block_size=file_block_size, consistency="size"
        ) as f:
            assert f.blocksize == file_block_size
            assert isinstance(f.checker, SizeChecker)


@pytest.mark.parametrize("bucket_type_val", list(BucketType))
def test_open_uses_default_blocksize_and_consistency_from_fs(
    extended_gcsfs, gcs_bucket_mocks, bucket_type_val
):
    csv_file = "2014-01-01.csv"
    csv_file_path = f"{TEST_ZONAL_BUCKET}/{csv_file}"
    csv_data = csv_files[csv_file]

    with gcs_bucket_mocks(csv_data, bucket_type_val=bucket_type_val):
        with extended_gcsfs.open(csv_file_path, "rb") as f:
            assert f.blocksize == extended_gcsfs.default_block_size
            assert type(f.checker) is ConsistencyChecker


def test_read_small_zb(extended_gcsfs, gcs_bucket_mocks):
    csv_file = "2014-01-01.csv"
    csv_file_path = f"{TEST_ZONAL_BUCKET}/{csv_file}"
    csv_data = csv_files[csv_file]

    with gcs_bucket_mocks(
        csv_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        with extended_gcsfs.open(csv_file_path, "rb", block_size=10) as f:
            out = []
            i = 1
            while True:
                i += 1
                data = f.read(3)
                if data == b"":
                    break
                out.append(data)
            assert extended_gcsfs.cat(csv_file_path) == b"".join(out)
            # cache drop
            assert len(f.cache.cache) < len(out)
            if mocks:
                mocks["sync_lookup_bucket_type"].assert_called_once_with(
                    TEST_ZONAL_BUCKET
                )


def test_readline_zb(extended_gcsfs, gcs_bucket_mocks):
    all_items = chain.from_iterable(
        [files.items(), csv_files.items(), text_files.items()]
    )
    for k, data in all_items:
        with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
            with extended_gcsfs.open("/".join([TEST_ZONAL_BUCKET, k]), "rb") as f:
                result = f.readline()
                expected = data.split(b"\n")[0] + (b"\n" if data.count(b"\n") else b"")
            assert result == expected


def test_readline_from_cache_zb(extended_gcsfs, gcs_bucket_mocks):
    data = b"a,b\n11,22\n3,4"
    if not extended_gcsfs.on_google:
        with mock.patch.object(
            extended_gcsfs, "_sync_lookup_bucket_type", return_value=BucketType.UNKNOWN
        ):
            with extended_gcsfs.open(a, "wb") as f:
                f.write(data)
    with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        with extended_gcsfs.open(a, "rb") as f:
            result = f.readline()
            assert result == b"a,b\n"
            assert f.loc == 4
            assert f.cache.cache == data

            result = f.readline()
            assert result == b"11,22\n"
            assert f.loc == 10
            assert f.cache.cache == data

            result = f.readline()
            assert result == b"3,4"
            assert f.loc == 13
            assert f.cache.cache == data


def test_readline_empty_zb(extended_gcsfs, gcs_bucket_mocks):
    data = b""
    if not extended_gcsfs.on_google:
        with mock.patch.object(
            extended_gcsfs, "_sync_lookup_bucket_type", return_value=BucketType.UNKNOWN
        ):
            with extended_gcsfs.open(b, "wb") as f:
                f.write(data)
    with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        with extended_gcsfs.open(b, "rb") as f:
            result = f.readline()
            assert result == data


def test_readline_blocksize_zb(extended_gcsfs, gcs_bucket_mocks):
    data = b"ab\n" + b"a" * (2**18) + b"\nab"
    if not extended_gcsfs.on_google:
        with mock.patch.object(
            extended_gcsfs, "_sync_lookup_bucket_type", return_value=BucketType.UNKNOWN
        ):
            with extended_gcsfs.open(c, "wb") as f:
                f.write(data)
    with gcs_bucket_mocks(data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL):
        with extended_gcsfs.open(c, "rb", block_size=2**18) as f:
            result = f.readline()
            expected = b"ab\n"
            assert result == expected

            result = f.readline()
            expected = b"a" * (2**18) + b"\n"
            assert result == expected

            result = f.readline()
            expected = b"ab"
            assert result == expected


@pytest.mark.parametrize(
    "start,end,exp_offset,exp_length,exp_exc",
    [
        (None, None, 0, file_size, None),  # full file
        (-10, None, file_size - 10, 10, None),  # start negative
        (10, -10, 10, file_size - 20, None),  # end negative
        (20, 20, 20, 0, None),  # zero-length slice
        (50, 40, None, None, ValueError),  # end before start -> raises
        (-200, None, None, None, ValueError),  # offset negative -> raises
        (file_size - 10, 200, file_size - 10, 10, None),  # end > size clamps
        (
            file_size + 10,
            file_size + 20,
            file_size + 10,
            0,
            None,
        ),  # offset > size -> empty
    ],
)
def test_process_limits_parametrized(
    extended_gcsfs, start, end, exp_offset, exp_length, exp_exc
):
    if exp_exc is not None:
        with pytest.raises(exp_exc):
            extended_gcsfs.sync_process_limits_to_offset_and_length(
                file_path, start, end
            )
    else:
        offset, length = extended_gcsfs.sync_process_limits_to_offset_and_length(
            file_path, start, end
        )
        assert offset == exp_offset
        assert length == exp_length


@pytest.mark.parametrize(
    "exception_to_raise",
    [ValueError, DataCorruption, Exception],
)
def test_mrd_exception_handling(extended_gcsfs, gcs_bucket_mocks, exception_to_raise):
    """
    Tests that _cat_file correctly propagates exceptions from mrd.download_ranges.
    """
    with gcs_bucket_mocks(
        json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        if extended_gcsfs.on_google:
            pytest.skip("Cannot mock exceptions on real GCS")

        # Configure the mock to raise a specified exception
        if exception_to_raise is DataCorruption:
            # The first argument is 'response', the message is in '*args'
            mocks["downloader"].download_ranges.side_effect = exception_to_raise(
                None, "Test exception raised"
            )
        else:
            mocks["downloader"].download_ranges.side_effect = exception_to_raise(
                "Test exception raised"
            )

        with pytest.raises(exception_to_raise, match="Test exception raised"):
            extended_gcsfs.read_block(file_path, 0, 10)

        mocks["downloader"].download_ranges.assert_called_once()


def test_mrd_stream_cleanup(extended_gcsfs, gcs_bucket_mocks):
    """
    Tests that mrd stream is properly closed with file closure.
    """
    with gcs_bucket_mocks(
        json_data, bucket_type_val=BucketType.ZONAL_HIERARCHICAL
    ) as mocks:
        if not extended_gcsfs.on_google:

            def close_side_effect():
                mocks["downloader"].is_stream_open = False

            mocks["downloader"].close.side_effect = close_side_effect

        with extended_gcsfs.open(file_path, "rb") as f:
            assert f.mrd is not None

        assert True is f.closed
        assert False is f.mrd.is_stream_open


class TestExtendedGcsFileSystemMv:
    """Unit tests for the _mv method in ExtendedGcsFileSystem."""

    rename_success_params = [
        pytest.param("old_dir", "new_dir", id="simple_rename_at_root"),
        pytest.param(
            "nested/old_dir",
            "nested/new_dir",
            id="rename_within_nested_dir",
        ),
    ]

    @pytest.mark.parametrize("path1, path2", rename_success_params)
    def test_hns_folder_rename_success(self, gcs_hns, gcs_hns_mocks, path1, path2):
        """Test successful HNS folder rename."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/{path1}"
        path2 = f"{TEST_HNS_BUCKET}/{path2}"

        # Setup a more complex directory structure
        file_in_root = f"{path1}/file1.txt"
        nested_file = f"{path1}/sub_dir/file2.txt"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # Configure mocks
                # 1. First _info call in _mv on path1 should succeed.
                # 2. _info call in exists(path1) after mv should fail.
                # 3. _info call in exists(path2) after mv should succeed.
                mocks["info"].side_effect = [
                    {"type": "directory", "name": path1},
                    FileNotFoundError(path1),
                    {"type": "directory", "name": path2},
                    {"type": "file", "name": f"{path2}/file1.txt"},
                    {"type": "file", "name": f"{path2}/sub_dir/file2.txt"},
                ]

            gcsfs.touch(file_in_root)
            gcsfs.touch(nested_file)
            gcsfs.mv(path1, path2)

            # Verify that the old path no longer exist
            assert not gcsfs.exists(path1)

            # Verify that the new paths exist
            assert gcsfs.exists(path2)
            assert gcsfs.exists(f"{path2}/file1.txt")
            assert gcsfs.exists(f"{path2}/sub_dir/file2.txt")

        if mocks:
            mocks["async_lookup_bucket_type"].assert_called_once_with(TEST_HNS_BUCKET)
            # Verify the sequence of _info calls for mv and exists checks.
            expected_info_calls = [
                mock.call(path1),  # from mv
                mock.call(path1),  # from exists(path1)
                mock.call(path2),  # from exists(path2)
            ]
            mocks["info"].assert_has_awaits(expected_info_calls)
            mocks["control_client"].rename_folder.assert_called()
            mocks["super_mv"].assert_not_called()
            # Verify that invalidate_cache was called for the old path, new path,
            # and their parents.
            mocks["invalidate_cache"].assert_has_calls(
                [
                    mock.call(path1),
                    mock.call(path2),
                    mock.call(gcsfs._parent(path1)),
                    mock.call(gcsfs._parent(path2)),
                ],
                any_order=True,
            )

    @pytest.mark.skipif(
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com",
        reason=(
            "Skipping on real GCS because info throws FileNotFoundError for empty directories on HNS buckets."
        ),
    )
    def test_hns_empty_folder_rename_success(self, gcs_hns, gcs_hns_mocks):
        """Test successful HNS rename of an empty folder."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/empty_old_dir"
        path2 = f"{TEST_HNS_BUCKET}/empty_new_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # Configure mocks for the sequence of calls
                mocks["info"].side_effect = [
                    {"type": "directory", "name": path1},  # _mv check
                    FileNotFoundError(path1),  # exists(path1) after move
                    {"type": "directory", "name": path2},  # exists(path2) after move
                ]

            # Simulate creating an empty directory by creating and then deleting a file inside a
            # folder as mkdir is still not supported on HNS buckets.
            placeholder_file = f"{path1}/placeholder.txt"
            gcsfs.touch(placeholder_file)
            gcsfs.rm(placeholder_file)

            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_called_once_with(
                    TEST_HNS_BUCKET
                )
                expected_info_calls = [
                    mock.call(path1),  # from _mv
                    mock.call(path1),  # from exists(path1)
                    mock.call(path2),  # from exists(path2)
                ]
                mocks["info"].assert_has_awaits(expected_info_calls)
                mocks["control_client"].rename_folder.assert_called_once()
                mocks["super_mv"].assert_not_called()
                mocks["invalidate_cache"].assert_has_calls(
                    [
                        mock.call(path1),
                        mock.call(path2),
                        mock.call(gcsfs._parent(path1)),
                        mock.call(gcsfs._parent(path2)),
                    ]
                )

    @pytest.mark.parametrize(
        "bucket_type, info_return, path1, path2, reason",
        [
            (
                BucketType.HIERARCHICAL,
                {"type": "file"},
                "f",
                "f2",
                "is a file",
            ),
            (
                BucketType.NON_HIERARCHICAL,
                {"type": "directory"},
                "d",
                "d2",
                "not an HNS bucket",
            ),
            pytest.param(
                BucketType.HIERARCHICAL,
                {"type": "directory"},
                "d",
                "another-bucket/d",
                "different bucket",
                marks=pytest.mark.xfail(
                    reason="Cross-bucket move not fully supported in test setup"
                ),
            ),
            (
                BucketType.HIERARCHICAL,
                {"type": "directory"},
                "d",
                "",  # Root of bucket
                "destination is bucket root",
            ),
        ],
    )
    def test_fallback_to_super_mv(
        self,
        gcs_hns,
        gcs_hns_mocks,
        bucket_type,
        info_return,
        path1,
        path2,
        reason,
    ):
        """Test scenarios that should fall back to the parent's mv method."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/{path1}"
        # Handle cross-bucket case where path2 already includes the bucket
        if "/" in path2:
            path2 = path2
        else:
            path2 = (
                f"{TEST_HNS_BUCKET}/{path2}" if path2 != "" else f"{TEST_HNS_BUCKET}/"
            )
        with gcs_hns_mocks(bucket_type, gcsfs) as mocks:
            if mocks:
                if bucket_type in [
                    BucketType.HIERARCHICAL,
                    BucketType.ZONAL_HIERARCHICAL,
                ]:
                    mocks["info"].side_effect = [
                        info_return,
                        FileNotFoundError(path1),
                        {"type": "file", "name": path2},
                    ]
                else:
                    mocks["info"].side_effect = [
                        FileNotFoundError(path1),
                        {"type": "file", "name": path2},
                    ]

            gcsfs.touch(path1)
            gcsfs.mv(path1, path2)

            assert not gcsfs.exists(path1)
            assert gcsfs.exists(path2)

            if mocks:
                mocks["control_client"].rename_folder.assert_not_called()
                mocks["super_mv"].assert_called_once_with(path1, path2)

    def test_mv_same_path_is_noop(self, gcs_hns, gcs_hns_mocks):
        """Test that mv with the same source and destination path is a no-op."""
        gcsfs = gcs_hns
        path = f"{TEST_HNS_BUCKET}/some_path"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            gcsfs.mv(path, path)

            if mocks:
                mocks["async_lookup_bucket_type"].assert_not_called()
                mocks["info"].assert_not_called()
                mocks["control_client"].rename_folder.assert_not_called()
                mocks["super_mv"].assert_not_called()

    def test_hns_rename_fails_if_parent_dne(self, gcs_hns, gcs_hns_mocks):
        """Test that HNS rename fails if the destination's parent does not exist."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir_to_move"
        path2 = f"{TEST_HNS_BUCKET}/new_parent/new_name"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # Mocked environment assertions
                mocks["info"].return_value = {"type": "directory"}
                mocks["control_client"].rename_folder.side_effect = (
                    api_exceptions.FailedPrecondition(
                        "The parent folder does not exist."
                    )
                )

            gcsfs.touch(f"{path1}/file.txt")

            # The underlying API error includes the status code (400) in its string representation.
            expected_msg = "HNS rename failed: 400 The parent folder does not exist."
            with pytest.raises(OSError, match=expected_msg):
                gcsfs.mv(path1, path2)

            if mocks:
                mocks["control_client"].rename_folder.assert_called()
                mocks["super_mv"].assert_not_called()

    def test_hns_rename_raises_file_not_found(self, gcs_hns, gcs_hns_mocks):
        """Test that NotFound from API raises FileNotFoundError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dne"
        path2 = f"{TEST_HNS_BUCKET}/new_dir"
        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["info"].side_effect = FileNotFoundError(path1)

            with pytest.raises(FileNotFoundError):
                gcsfs.mv(path1, path2)

            if mocks:
                mocks["super_mv"].assert_not_called()
                mocks["control_client"].rename_folder.assert_not_called()

    def test_hns_rename_raises_file_not_found_on_race_condition(
        self, gcs_hns, gcs_hns_mocks
    ):
        """Test that api_exceptions.NotFound from rename call raises FileNotFoundError."""
        is_real_gcs = (
            os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
        )
        if is_real_gcs:
            pytest.skip(
                "Cannot simulate race condition for rename against real GCS endpoint."
            )
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir_disappears"
        path2 = f"{TEST_HNS_BUCKET}/new_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                # Simulate _info finding the directory
                mocks["info"].return_value = {"type": "directory"}
                # Simulate the directory being gone when rename_folder is called
                mocks["control_client"].rename_folder.side_effect = (
                    api_exceptions.NotFound("Folder not found during rename")
                )

            with pytest.raises(FileNotFoundError, match="Source .* not found"):
                gcsfs.mv(path1, path2)

            if mocks:
                mocks["control_client"].rename_folder.assert_called_once()
                mocks["super_mv"].assert_not_called()

    def test_hns_rename_raises_os_error(self, gcs_hns, gcs_hns_mocks):
        """Test that FailedPrecondition from API raises OSError."""
        gcsfs = gcs_hns
        path1 = f"{TEST_HNS_BUCKET}/dir"
        path2 = f"{TEST_HNS_BUCKET}/existing_dir"

        with gcs_hns_mocks(BucketType.HIERARCHICAL, gcsfs) as mocks:
            if mocks:
                mocks["info"].return_value = {"type": "directory"}
                mocks["control_client"].rename_folder.side_effect = (
                    api_exceptions.Conflict("HNS rename failed due to conflict for")
                )

            gcsfs.touch(f"{path1}/file.txt")
            gcsfs.touch(f"{path2}/file.txt")

            expected_msg = (
                f"HNS rename failed due to conflict for '{path1}' to '{path2}'"
            )
            with pytest.raises(FileExistsError, match=expected_msg):
                gcsfs.mv(path1, path2)

            if mocks:
                mocks["super_mv"].assert_not_called()
                mocks["control_client"].rename_folder.assert_called()
