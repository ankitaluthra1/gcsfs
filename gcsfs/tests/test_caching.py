import asyncio
from unittest import mock

import fsspec.asyn
import pytest

from gcsfs.caching import Prefetcher, ReadAheadChunked


class MockVectorFetcher:
    """Simulates a backend capable of vector reads (accepting chunk_lengths)."""

    def __init__(self, data: bytes):
        self.data = data
        self.call_log = []

    def __call__(self, start, chunk_lengths):
        self.call_log.append({"start": start, "chunk_lengths": chunk_lengths})
        results = []
        current = start
        for length in chunk_lengths:
            end = min(current + length, len(self.data))
            results.append(self.data[current:end])
            current += length
        return results


@pytest.fixture
def source_data():
    """Generates 100 bytes of sequential data."""
    return bytes(range(100))


@pytest.fixture
def cache_setup(source_data):
    """Returns a tuple of (cache_instance, fetcher_mock)."""
    fetcher = MockVectorFetcher(source_data)
    # Blocksize 10, File size 100
    cache = ReadAheadChunked(blocksize=10, fetcher=fetcher, size=100)
    return cache, fetcher


def test_initial_state(cache_setup):
    cache, _ = cache_setup
    assert cache.cache == b""
    assert len(cache.chunks) == 0
    assert cache.hit_count == 0
    assert cache.miss_count == 0


def test_fetch_with_readahead(cache_setup, source_data):
    """Test a basic fetch. Should retrieve requested data + blocksize readahead."""
    cache, fetcher = cache_setup

    # Request bytes 0-5
    result = cache._fetch(0, 5)

    # 1. Verify data correctness
    assert result == source_data[0:5]

    # 2. Verify Fetcher calls
    # Should fetch requested (5) + readahead (10)
    assert len(fetcher.call_log) == 1
    assert fetcher.call_log[0]["start"] == 0
    assert fetcher.call_log[0]["chunk_lengths"] == [5, 10]

    # 3. Verify Internal State (Deque)
    # We expect two chunks: the requested part (0-5) and readahead (5-15)
    assert len(cache.chunks) == 2
    assert cache.chunks[0] == (0, 5, source_data[0:5])
    assert cache.chunks[1] == (5, 15, source_data[5:15])

    # 4. Verify compatibility property
    assert cache.cache == source_data[0:15]


def test_cache_hit_fully_contained(cache_setup, source_data):
    """Test fetching data that is already inside the readahead buffer."""
    cache, fetcher = cache_setup

    # Prime the cache (fetch 0-5, readahead 5-15)
    cache._fetch(0, 5)

    # Reset call log to ensure next fetch doesn't hit backend
    fetcher.call_log = []

    # Request 5-10 (Should be inside the readahead chunk)
    result = cache._fetch(5, 10)

    assert result == source_data[5:10]
    assert len(fetcher.call_log) == 0  # No backend calls
    assert cache.hit_count == 1


def test_cache_hit_spanning_chunks(cache_setup, source_data):
    """Test fetching data that spans across the requested chunk and the readahead chunk."""
    cache, fetcher = cache_setup

    # Prime cache: Chunk 1 (0-5), Chunk 2 (5-15)
    cache._fetch(0, 5)

    # Request 2-8 (Spans Chunk 1 and Chunk 2)
    result = cache._fetch(2, 8)

    assert result == source_data[2:8]
    # Should join parts internally without fetching new data
    assert cache.hit_count == 1
    assert len(fetcher.call_log) == 1  # Only the initial prime call


def test_backward_seek_clears_cache(cache_setup, source_data):
    """Test that seeking backwards (before current window) clears cache and refetches."""
    cache, fetcher = cache_setup

    # Prime cache at 50-60 (Readahead 60-70)
    cache._fetch(50, 60)
    assert cache.chunks[0][0] == 50

    # Seek backwards to 20
    fetcher.call_log = []
    result = cache._fetch(20, 30)

    assert result == source_data[20:30]
    # Cache should have cleared and fetched new
    assert fetcher.call_log[0]["start"] == 20
    assert cache.chunks[0][0] == 20


def test_forward_seek_miss(cache_setup, source_data):
    """Test requesting data far ahead of the current window."""
    cache, fetcher = cache_setup

    # Prime 0-5
    cache._fetch(0, 5)

    # Jump to 50
    fetcher.call_log = []
    result = cache._fetch(50, 55)

    assert result == source_data[50:55]
    # Should clear old chunks and fetch new
    assert len(cache.chunks) == 2  # 50-55 and readahead
    assert cache.chunks[0][0] == 50


def test_zero_copy_optimization(cache_setup, source_data):
    """Verify that if we request a chunk exactly, it returns the original object without slicing (identity check)."""
    cache, _ = cache_setup

    # Prime cache: Chunks will be (0, 5, data) and (5, 15, data)
    cache._fetch(0, 5)

    # Fetch exactly the second chunk (readahead buffer)
    # The logic inside _fetch has a check: if slice_start==0 and slice_end==len...
    exact_chunk = cache._fetch(5, 15)

    # Verify values
    assert exact_chunk == source_data[5:15]

    # Verify Identity (Zero Copy)
    # Note: string/bytes literals might be interned, but since we slice from source_data,
    # identity checks on the deque contents vs result should pass if logic holds.
    stored_readahead = cache.chunks[1][2]
    assert exact_chunk is stored_readahead


def test_end_of_file_truncation(cache_setup, source_data):
    """Ensure readahead doesn't go past file size."""
    cache, fetcher = cache_setup
    # File size is 100.

    # Fetch 95-100.
    # missing_len = 5.
    # readahead would usually be 10, but file ends at 100.
    result = cache._fetch(95, 100)

    assert result == source_data[95:100]
    assert len(fetcher.call_log) == 1

    # Check lengths requested.
    # Request: 5 bytes. Remaining space: 0. Readahead should be 0.
    args = fetcher.call_log[0]
    assert args["start"] == 95
    # Should only request the 5 bytes needed, no readahead
    assert args["chunk_lengths"] == [5]

    # Ensure no empty readahead chunk was added
    assert len(cache.chunks) == 1


def test_none_arguments(cache_setup, source_data):
    """Test behavior when start/end are None."""
    cache, _ = cache_setup

    result = cache._fetch(None, None)
    assert len(result) == 100
    assert result == source_data


def test_out_of_bounds(cache_setup):
    """Test start >= size returns empty."""
    cache, _ = cache_setup
    assert cache._fetch(150, 200) == b""


class TrackedAsyncMockFetcher:
    """Simulates an async backend and tracks calls for assertions."""

    def __init__(self, data: bytes):
        self.data = data
        self.should_fail = False
        self.calls = []

    async def __call__(self, start, size, split_factor=1):
        self.calls.append({"start": start, "size": size, "split_factor": split_factor})
        if self.should_fail:
            raise RuntimeError("Mocked network error")

        # Simulate slight network delay to allow the event loop to switch contexts
        await asyncio.sleep(0.001)
        end = min(start + size, len(self.data))
        return self.data[start:end]


@pytest.fixture
def prefetcher_source_data():
    """Generates 100 bytes of predictable sequential data."""
    return bytes(range(100))


@pytest.fixture
def prefetcher_setup(prefetcher_source_data):
    """Provides a fresh Prefetcher and its mocked fetcher for each test."""
    fetcher = TrackedAsyncMockFetcher(prefetcher_source_data)

    cache = Prefetcher(
        blocksize=10,
        fetcher=fetcher,
        size=len(prefetcher_source_data),
        max_prefetch_size=30,
        concurrency=4,
    )
    yield cache, fetcher
    cache.close()


def test_prefetcher_initial_state(prefetcher_setup):
    cache, _ = prefetcher_setup
    assert cache.user_offset == 0
    assert cache.sequential_streak == 0
    assert not cache.is_stopped


def test_prefetcher_sequential_reads(prefetcher_setup, source_data):
    cache, _ = prefetcher_setup

    res1 = cache._fetch(0, 15)
    assert res1 == source_data[0:15]
    assert cache.sequential_streak > 0
    res2 = cache._fetch(15, 25)
    assert res2 == source_data[15:25]


def test_prefetcher_out_of_bounds(prefetcher_setup):
    cache, _ = prefetcher_setup
    res = cache._fetch(250, 260)
    assert res == b""


def test_prefetcher_seek_resets_streak(prefetcher_setup, source_data):
    cache, _ = prefetcher_setup

    cache._fetch(0, 10)
    assert cache.sequential_streak > 0

    res = cache._fetch(50, 60)
    assert res == source_data[50:60]
    assert cache._buffer_offset == 60
    assert cache.user_offset >= 50


def test_prefetcher_exact_block_reads(prefetcher_setup, prefetcher_source_data):
    """Test reading exactly the blocksize increments streak and fetches correctly."""
    cache, fetcher = prefetcher_setup

    res1 = cache._fetch(0, 10)
    assert res1 == prefetcher_source_data[0:10]
    assert cache.sequential_streak == 1

    res2 = cache._fetch(10, 20)
    assert res2 == prefetcher_source_data[10:20]
    assert cache.sequential_streak == 2

    # Verify the background producer requested data in advance
    assert len(fetcher.calls) >= 2


def test_prefetcher_partial_read_and_remainder(
    prefetcher_setup, prefetcher_source_data
):
    """Test that reading smaller than blocksize caches the remainder for the next read."""
    cache, fetcher = prefetcher_setup

    # Fetch only 4 bytes. Blocksize is 10.
    res1 = cache._fetch(0, 4)
    assert res1 == prefetcher_source_data[0:4]

    # No bytes should be in remainder due to adaptive fetch
    assert len(cache._remainder) == 0
    assert cache._buffer_offset == 4

    # Fetch next 4 bytes (4 to 8).
    res2 = cache._fetch(4, 8)  # prefetching starts here
    assert res2 == prefetcher_source_data[4:8]
    assert cache._buffer_offset == 8
    assert len(cache._remainder) == 0

    res3 = cache._fetch(8, 10)
    assert res3 == prefetcher_source_data[8:10]
    assert len(cache._remainder) == 2
    assert cache._buffer_offset == 10


def test_prefetcher_cross_block_read(prefetcher_setup, prefetcher_source_data):
    """Test requesting a large chunk that spans multiple underlying prefetch blocks."""
    cache, _ = prefetcher_setup
    res = cache._fetch(0, 25)

    assert res == prefetcher_source_data[0:25]
    assert cache._buffer_offset == 25
    assert len(cache._remainder) == 0  # adaptive fetching.


def test_prefetcher_seek_same_offset(prefetcher_setup):
    """Test that seeking to the current user_offset is a no-op and does not clear buffers."""
    cache, _ = prefetcher_setup
    cache._fetch(0, 5)  # Read something to set user_offset and populate remainder
    streak_before = cache.sequential_streak
    remainder_before = cache._remainder

    # This should be a no-op
    fsspec.asyn.sync(cache.loop, cache.seek, cache.user_offset)
    assert cache.sequential_streak == streak_before
    assert cache._remainder == remainder_before


def test_prefetcher_eof_handling(prefetcher_setup, prefetcher_source_data):
    """Test behavior when fetching up to and past the file size limit."""
    cache, _ = prefetcher_setup
    res = cache._fetch(95, 110)
    assert res == prefetcher_source_data[95:100]
    assert cache._fetch(105, 115) == b""


def test_prefetcher_producer_error_propagation(prefetcher_setup):
    """Test that exceptions in the background fetcher task surface to the caller."""
    cache, fetcher = prefetcher_setup
    fetcher.should_fail = True
    with pytest.raises(RuntimeError, match="Mocked network error"):
        cache._fetch(0, 10)
    assert cache.is_stopped is True


def test_prefetcher_dynamic_split_factor(prefetcher_setup, prefetcher_source_data):
    """Test that split_factor increases for large chunks on sequential reads."""
    cache, fetcher = prefetcher_setup

    with mock.patch.object(Prefetcher, "MIN_CHUNK_SIZE", 5):
        cache._fetch(0, 10)
        cache._fetch(10, 20)

        fsspec.asyn.sync(cache.loop, asyncio.sleep, 0.05)

    recent_calls = [c for c in fetcher.calls if c["start"] >= 20]
    assert len(recent_calls) > 0
    assert recent_calls[0]["split_factor"] > 1


def test_prefetcher_max_prefetch_limit(prefetcher_setup):
    """Test that the producer pauses when the queue hits the max_prefetch_size."""
    cache, _ = prefetcher_setup
    cache._fetch(0, 1)
    fsspec.asyn.sync(cache.loop, asyncio.sleep, 0.05)
    max_expected_offset = cache.user_offset + cache.max_prefetch_size + cache.blocksize
    assert cache.current_offset <= max_expected_offset


def test_prefetcher_close_while_active(prefetcher_setup):
    """Test that closing the prefetcher safely cancels pending background tasks."""
    cache, _ = prefetcher_setup
    cache._fetch(0, 10)  # prefetching do not start on first read.
    cache._fetch(10, 20)  # prefetching start here

    assert len(cache._active_tasks) > 0 or not cache.queue.empty()
    assert cache.is_stopped is False

    cache.close()

    assert cache.is_stopped is True
    assert len(cache._active_tasks) == 0
    assert cache.queue.empty() is True


def test_prefetcher_adaptive_averaging(prefetcher_setup):
    """Verify that the blocksize adapts to the average of the last 10 reads."""
    cache, _ = prefetcher_setup
    assert cache._get_adaptive_blocksize() == 10
    for _ in range(5):
        cache._fetch(cache.user_offset, cache.user_offset + 5)

    assert cache._get_adaptive_blocksize() == 5
    for _ in range(5):
        cache._fetch(cache.user_offset, cache.user_offset + 2)
    assert cache._get_adaptive_blocksize() == 3


def test_prefetcher_history_eviction(prefetcher_setup):
    """Verify that only the last 10 reads impact the adaptive blocksize."""
    cache, _ = prefetcher_setup
    for _ in range(10):
        cache._fetch(cache.user_offset, cache.user_offset + 1)

    assert cache.history_sum == 10
    assert len(cache.read_history) == 10
    cache._fetch(cache.user_offset, cache.user_offset + 10)
    assert cache.history_sum == 19
    assert cache.read_history[-1] == 10


def test_prefetcher_seek_resets_history(prefetcher_setup):
    """Verify that a seek clears adaptive history to prevent stale logic."""
    cache, _ = prefetcher_setup
    cache._fetch(0, 100)
    cache._fetch(100, 200)
    assert cache.history_sum > 0

    fsspec.asyn.sync(cache.loop, cache.seek, 500)
    assert cache.history_sum == 0
    assert len(cache.read_history) == 0
    assert cache._get_adaptive_blocksize() == cache.blocksize


def test_producer_loop_uses_adaptive_size(prefetcher_setup, prefetcher_source_data):
    """Verify the producer actually fetches using the adaptive blocksize."""
    cache, fetcher = prefetcher_setup

    with mock.patch.object(cache, "_get_adaptive_blocksize", return_value=7):
        cache._wakeup_producer.set()
        fsspec.asyn.sync(cache.loop, asyncio.sleep, 0.1)

        prefetch_calls = [c for c in fetcher.calls]
        assert len(prefetch_calls) > 0
        assert prefetch_calls[-1]["size"] == 7
