import concurrent.futures
import ctypes
from unittest import mock

import pytest

from gcsfs import zb_hns_utils
from gcsfs.zb_hns_utils import DirectMemmoveBuffer, MRDPool

mock_grpc_client = mock.Mock()
bucket_name = "test-bucket"
object_name = "test-object"
generation = "12345"


@pytest.mark.asyncio
async def test_download_range():
    """
    Tests that download_range calls mrd.download_ranges with the correct
    parameters and returns the data written to the buffer.
    """
    offset = 10
    length = 20
    mock_mrd = mock.AsyncMock()
    expected_data = b"test data from download"

    async def mock_download_ranges(ranges):
        _offset, _length, buffer = ranges[0]
        buffer.write(expected_data)

    mock_mrd.download_ranges.side_effect = mock_download_ranges

    result = await zb_hns_utils.download_range(offset, length, mock_mrd)

    mock_mrd.download_ranges.assert_called_once_with([(offset, length, mock.ANY)])
    assert result == expected_data


@pytest.mark.asyncio
async def test_init_aaow():
    """
    Tests that init_aaow calls the underlying AsyncAppendableObjectWriter.open
    method and returns its result.
    """
    mock_writer_instance = mock.AsyncMock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
        new_callable=mock.Mock,
        return_value=mock_writer_instance,
    ) as mock_writer_class:
        result = await zb_hns_utils.init_aaow(
            mock_grpc_client, bucket_name, object_name, generation
        )

        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=generation,
            writer_options={},
        )
        mock_writer_instance.open.assert_awaited_once()
        assert result is mock_writer_instance


@pytest.mark.asyncio
async def test_init_aaow_with_flush_interval_bytes():
    """
    Tests that init_aaow correctly passes the flush_interval_bytes
    parameter to the AsyncAppendableObjectWriter.
    """
    mock_writer_instance = mock.AsyncMock()
    with mock.patch(
        "gcsfs.zb_hns_utils.AsyncAppendableObjectWriter",
        new_callable=mock.Mock,
        return_value=mock_writer_instance,
    ) as mock_writer_class:
        result = await zb_hns_utils.init_aaow(
            mock_grpc_client,
            bucket_name,
            object_name,
            generation,
            flush_interval_bytes=1024,
        )

        mock_writer_class.assert_called_once_with(
            client=mock_grpc_client,
            bucket_name=bucket_name,
            object_name=object_name,
            generation=generation,
            writer_options={"FLUSH_INTERVAL_BYTES": 1024},
        )
        mock_writer_instance.open.assert_awaited_once()
        assert result is mock_writer_instance


@pytest.mark.asyncio
async def test_mrd_pool_scaling():
    gcsfs_mock = mock.Mock()
    gcsfs_mock._get_grpc_client = mock.AsyncMock()

    mrd_instance_mock = mock.AsyncMock()
    mrd_instance_mock.persisted_size = 1024

    with mock.patch(
        "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
        new_callable=mock.AsyncMock,
        return_value=mrd_instance_mock,
    ) as create_mrd_mock:

        pool = MRDPool(gcsfs_mock, "bucket", "obj", "123", pool_size=2)

        await pool.initialize()
        assert pool.persisted_size == 1024
        assert pool._active_count == 1
        create_mrd_mock.assert_awaited_once()

        async with pool.get_mrd() as mrd1:
            assert mrd1 == mrd_instance_mock

            async with pool.get_mrd() as _:
                assert pool._active_count == 2
                assert create_mrd_mock.call_count == 2

        assert pool._free_mrds.qsize() == 2


@pytest.mark.asyncio
async def test_mrd_pool_close():
    gcsfs_mock = mock.Mock()
    gcsfs_mock._get_grpc_client = mock.AsyncMock()

    mrd_instance_mock = mock.AsyncMock()

    with mock.patch(
        "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
        return_value=mrd_instance_mock,
    ):
        pool = MRDPool(gcsfs_mock, "bucket", "obj", "123", pool_size=1)
        await pool.initialize()

        await pool.close()
        mrd_instance_mock.close.assert_awaited_once()
        assert len(pool._all_mrds) == 0


def test_direct_memmove_buffer():
    import concurrent.futures

    size = 20
    buffer_array = (ctypes.c_char * size)()
    start_address = ctypes.addressof(buffer_array)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    buf = DirectMemmoveBuffer(start_address, executor, max_pending=2)

    data1 = b"hello"
    data2 = b"world"

    future1 = buf.write(data1)
    future2 = buf.write(data2)

    future1.result()
    future2.result()
    buf.close()

    result_bytes = ctypes.string_at(start_address, len(data1) + len(data2))
    assert result_bytes == b"helloworld"

    executor.shutdown()


@pytest.fixture
def mock_gcsfs():
    gcsfs_mock = mock.Mock()
    gcsfs_mock._get_grpc_client = mock.AsyncMock()
    return gcsfs_mock


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_scaling(create_mrd_mock, mock_gcsfs):
    mrd_instance_mock = mock.AsyncMock()
    mrd_instance_mock.persisted_size = 1024
    create_mrd_mock.return_value = mrd_instance_mock

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=2)

    await pool.initialize()
    assert pool.persisted_size == 1024
    assert pool._active_count == 1
    create_mrd_mock.assert_awaited_once()

    async with pool.get_mrd() as mrd1:
        assert mrd1 == mrd_instance_mock

        # Since mrd1 is in use, getting another one should spawn a new MRD
        async with pool.get_mrd() as _:
            assert pool._active_count == 2
            assert create_mrd_mock.call_count == 2

    # Both should have been returned to the free queue
    assert pool._free_mrds.qsize() == 2


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_double_initialize(create_mrd_mock, mock_gcsfs):
    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=2)

    await pool.initialize()
    await pool.initialize()  # Second call should be a no-op

    assert pool._active_count == 1
    create_mrd_mock.assert_awaited_once()


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_get_mrd_creation_error(create_mrd_mock, mock_gcsfs):
    # First creation succeeds during initialization
    valid_mrd = mock.AsyncMock()

    # Second creation fails when pool tries to scale
    create_mrd_mock.side_effect = [valid_mrd, Exception("Network Error")]

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=2)
    await pool.initialize()

    # Consume the initialized MRD
    async def consume_and_error():
        async with pool.get_mrd() as _:
            # Try to get a second one, which forces a spawn that will fail
            with pytest.raises(Exception, match="Network Error"):
                async with pool.get_mrd() as _:
                    pass

    await consume_and_error()

    # Active count should remain 1 because the second creation failed and rolled back
    assert pool._active_count == 1


@pytest.mark.asyncio
@mock.patch(
    "google.cloud.storage.asyncio.async_multi_range_downloader.AsyncMultiRangeDownloader.create_mrd",
    new_callable=mock.AsyncMock,
)
async def test_mrd_pool_close_with_exceptions(create_mrd_mock, mock_gcsfs):
    bad_mrd_instance = mock.AsyncMock()
    bad_mrd_instance.close.side_effect = Exception("Close failed")
    create_mrd_mock.return_value = bad_mrd_instance

    pool = MRDPool(mock_gcsfs, "bucket", "obj", "123", pool_size=1)
    await pool.initialize()

    # Should not raise an exception, even though the internal close() fails
    await pool.close()

    bad_mrd_instance.close.assert_awaited_once()
    assert len(pool._all_mrds) == 0


def test_direct_memmove_buffer_success():
    size = 20
    buffer_array = (ctypes.c_char * size)()
    start_address = ctypes.addressof(buffer_array)

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    buf = DirectMemmoveBuffer(start_address, executor, max_pending=2)

    data1 = b"hello"
    data2 = b"world"

    future1 = buf.write(data1)
    future2 = buf.write(data2)

    future1.result()
    future2.result()
    buf.close()

    result_bytes = ctypes.string_at(start_address, len(data1) + len(data2))
    assert result_bytes == b"helloworld"

    executor.shutdown()


@mock.patch("gcsfs.zb_hns_utils.ctypes.memmove")
def test_direct_memmove_buffer_error_handling(mock_memmove):
    size = 20
    buffer_array = (ctypes.c_char * size)()
    start_address = ctypes.addressof(buffer_array)

    # Simulate an access violation or similar error during memory copy
    mock_memmove.side_effect = MemoryError("Segfault simulated")

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    buf = DirectMemmoveBuffer(start_address, executor, max_pending=2)

    # First write triggers the background error
    future = buf.write(b"bad data")

    # Wait for the background thread to actually fail
    with pytest.raises(MemoryError):
        future.result()

    # Subsequent writes should raise the stored error immediately
    with pytest.raises(MemoryError, match="Segfault simulated"):
        buf.write(b"more data")

    # Close should also raise the stored error
    with pytest.raises(MemoryError, match="Segfault simulated"):
        buf.close()

    executor.shutdown()
