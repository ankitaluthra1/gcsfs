import io
import logging
import os
from itertools import chain
from unittest import mock

import pytest
from google.cloud.storage.exceptions import DataCorruption

from gcsfs.checkers import ConsistencyChecker, MD5Checker, SizeChecker
from gcsfs.extended_gcsfs import (
    BucketType,
    initiate_upload,
    simple_upload,
    upload_chunk,
)
from gcsfs.tests.conftest import csv_files, files, text_files
from gcsfs.tests.settings import TEST_ZONAL_BUCKET

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
                mocks["get_bucket_type"].assert_called_once_with(TEST_ZONAL_BUCKET)


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
            extended_gcsfs, "_get_bucket_type", return_value=BucketType.UNKNOWN
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
            extended_gcsfs, "_get_bucket_type", return_value=BucketType.UNKNOWN
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


@pytest.mark.asyncio
async def test_simple_upload_zonal(extended_gcsfs, zonal_write_mocks):
    """Test simple_upload for Zonal buckets calls the correct writer methods."""
    data = b"test data for simple_upload"
    rpath = f"{TEST_ZONAL_BUCKET}/simple_upload_test"
    await simple_upload(
        extended_gcsfs,
        bucket=TEST_ZONAL_BUCKET,
        key="test-obj",
        datain=data,
    )
    if zonal_write_mocks:
        zonal_write_mocks["init_aaow"].assert_awaited_once_with(
            extended_gcsfs.grpc_client, TEST_ZONAL_BUCKET, "test-obj"
        )
        zonal_write_mocks["aaow"].append.assert_awaited_once_with(data)
        zonal_write_mocks["aaow"].close.assert_awaited_once_with(finalize_on_close=True)
    else:
        assert extended_gcsfs.cat(rpath) == data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsupported_kwarg",
    [
        {"metadatain": {"key": "value"}},
        {"fixed_key_metadata": {"key": "value"}},
        {"kms_key_name": "key_name"},
        {"consistency": "md5"},
        {"content_type": "text/plain"},
    ],
)
async def test_simple_upload_zonal_unsupported_params(
    extended_gcsfs, zonal_write_mocks, unsupported_kwarg, caplog
):
    """Test simple_upload for Zonal buckets warns on unsupported parameters."""

    # Ensure caplog captures the warning by setting the level
    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await simple_upload(
            extended_gcsfs,
            bucket=TEST_ZONAL_BUCKET,
            key="test-obj",
            datain=b"",
            **unsupported_kwarg,
        )

    assert any(
        "will be ignored" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsupported_kwarg",
    [
        {"metadata": {"key": "value"}},
        {"fixed_key_metadata": {"key": "value"}},
        {"kms_key_name": "key_name"},
        {"content_type": "text/plain"},
    ],
)
async def test_initiate_upload_zonal_unsupported_params(
    extended_gcsfs, zonal_write_mocks, unsupported_kwarg, caplog
):
    """Test initiate_upload for Zonal buckets warns on unsupported parameters."""
    with caplog.at_level(logging.WARNING, logger="gcsfs"):
        await initiate_upload(
            fs=extended_gcsfs,
            bucket=TEST_ZONAL_BUCKET,
            key="test-obj",
            **unsupported_kwarg,
        )
    assert any(
        "will be ignored" in r.message and r.levelname == "WARNING"
        for r in caplog.records
    )


@pytest.mark.asyncio
async def test_initiate_upload_zonal(extended_gcsfs, zonal_write_mocks):
    """Test initiate_upload for Zonal buckets returns a writer instance."""
    writer = await initiate_upload(
        fs=extended_gcsfs, bucket=TEST_ZONAL_BUCKET, key="test-obj"
    )
    zonal_write_mocks["init_aaow"].assert_awaited_once_with(
        extended_gcsfs.grpc_client, TEST_ZONAL_BUCKET, "test-obj"
    )
    assert writer is zonal_write_mocks["aaow"]


@pytest.mark.asyncio
async def test_initiate_and_upload_chunk_zonal(extended_gcsfs, zonal_write_mocks):
    """Test upload_chunk for Zonal buckets appends data."""
    size_in_bytes = 1024  # 1KB
    data1 = os.urandom(size_in_bytes - 1)
    data2 = os.urandom(size_in_bytes)
    rpath = f"{TEST_ZONAL_BUCKET}/chunk_upload_test"
    writer = await initiate_upload(
        fs=extended_gcsfs, bucket=TEST_ZONAL_BUCKET, key="chunk_upload_test"
    )
    # Simulate some data already written
    await upload_chunk(
        fs=extended_gcsfs,
        location=writer,
        data=data1,
        offset=0,
        size=2048,
        content_type=None,
    )

    await upload_chunk(
        fs=extended_gcsfs,
        location=writer,
        data=data2,
        offset=0,
        size=2048,
        content_type=None,
    )
    assert writer.offset == (len(data1) + len(data2))
    if zonal_write_mocks:
        assert writer.append.await_args_list == [mock.call(data1), mock.call(data2)]
        writer.close.assert_not_awaited()
    else:
        assert extended_gcsfs.cat(rpath) == data1 + data2
        assert writer._is_stream_open


@pytest.mark.asyncio
async def test_upload_chunk_zonal_final_chunk(extended_gcsfs, zonal_write_mocks):
    """Test upload_chunk for Zonal buckets finalizes on the last chunk."""

    data = b"final chunk"
    rpath = f"{TEST_ZONAL_BUCKET}/final_chunk_test"
    writer = await initiate_upload(
        fs=extended_gcsfs, bucket=TEST_ZONAL_BUCKET, key="final_chunk_test"
    )

    await upload_chunk(
        fs=extended_gcsfs,
        location=writer,
        data=b"",
        offset=0,
        size=len(data),
        content_type=None,
    )  # Try uploading empty chunk
    await upload_chunk(
        fs=extended_gcsfs,
        location=writer,
        data=data,
        offset=0,
        size=len(data),
        content_type=None,
    )  # stream should be closed now

    with pytest.raises(
        ValueError, match="Writer is closed. Please initiate a new upload."
    ):
        await upload_chunk(
            fs=extended_gcsfs,
            location=writer,
            data=b"",
            offset=0,
            size=len(data),
            content_type=None,
        )
    assert writer.offset == len(data)
    if zonal_write_mocks:
        assert writer.append.await_args_list == [mock.call(b""), mock.call(data)]
        writer.close.assert_awaited_once_with(finalize_on_close=True)
    else:
        assert extended_gcsfs.cat(rpath) == data
        assert writer._is_stream_open is False


@pytest.mark.asyncio
async def test_upload_chunk_zonal_exception_cleanup(extended_gcsfs, zonal_write_mocks):
    """
    Tests that upload_chunk correctly closes the stream (cleanup) when an
    exception occurs during append, without finalizing the object.
    """
    if extended_gcsfs.on_google:
        pytest.skip("Cannot mock exceptions on real GCS")

    writer = await initiate_upload(
        fs=extended_gcsfs, bucket=TEST_ZONAL_BUCKET, key="final_chunk_test"
    )

    error_message = "Simulated network failure"
    writer.append.side_effect = Exception(error_message)

    with pytest.raises(Exception, match=error_message):
        await upload_chunk(
            fs=extended_gcsfs,
            location=writer,
            data=b"some data",
            offset=0,
            size=100,
            content_type=None,
        )

    writer.close.assert_awaited_once_with(finalize_on_close=False)


@pytest.mark.asyncio
async def test_upload_chunk_zonal_wrong_type(extended_gcsfs):
    """Test upload_chunk raises TypeError for incorrect location type."""
    with pytest.raises(TypeError, match="expects an AsyncAppendableObjectWriter"):
        await upload_chunk(
            fs=extended_gcsfs,
            location="not-a-writer",
            data=b"",
            offset=0,
            size=0,
            content_type=None,
        )
