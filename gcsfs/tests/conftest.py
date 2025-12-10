import logging
import os
import shlex
import subprocess
import time
import uuid
from contextlib import nullcontext
from unittest.mock import patch

import fsspec
import pytest
import requests
from google.cloud import storage

from gcsfs import GCSFileSystem
from gcsfs.tests.settings import TEST_BUCKET, TEST_VERSIONED_BUCKET, TEST_ZONAL_BUCKET

files = {
    "test/accounts.1.json": (
        b'{"amount": 100, "name": "Alice"}\n'
        b'{"amount": 200, "name": "Bob"}\n'
        b'{"amount": 300, "name": "Charlie"}\n'
        b'{"amount": 400, "name": "Dennis"}\n'
    ),
    "test/accounts.2.json": (
        b'{"amount": 500, "name": "Alice"}\n'
        b'{"amount": 600, "name": "Bob"}\n'
        b'{"amount": 700, "name": "Charlie"}\n'
        b'{"amount": 800, "name": "Dennis"}\n'
    ),
}

csv_files = {
    "2014-01-01.csv": (
        b"name,amount,id\n" b"Alice,100,1\n" b"Bob,200,2\n" b"Charlie,300,3\n"
    ),
    "2014-01-02.csv": b"name,amount,id\n",
    "2014-01-03.csv": (
        b"name,amount,id\n" b"Dennis,400,4\n" b"Edith,500,5\n" b"Frank,600,6\n"
    ),
}
text_files = {
    "nested/file1": b"hello\n",
    "nested/file2": b"world",
    "nested/nested2/file1": b"hello\n",
    "nested/nested2/file2": b"world",
}
allfiles = dict(**files, **csv_files, **text_files)
a = TEST_BUCKET + "/tmp/test/a"
b = TEST_BUCKET + "/tmp/test/b"
c = TEST_BUCKET + "/tmp/test/c"
d = TEST_BUCKET + "/tmp/test/d"

params = dict()


def stop_docker(container):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % container)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", "-v", cid])


@pytest.fixture(scope="session")
def docker_gcs():
    if "STORAGE_EMULATOR_HOST" in os.environ:
        # assume using real API or otherwise have a server already set up
        yield os.getenv("STORAGE_EMULATOR_HOST")
        return
    params["token"] = "anon"
    container = "gcsfs_test"
    cmd = (
        "docker run -d -p 4443:4443 --name gcsfs_test fsouza/fake-gcs-server:latest -scheme "
        "http -public-host 0.0.0.0:4443 -external-url http://localhost:4443 "
        "-backend memory"
    )
    stop_docker(container)
    subprocess.check_output(shlex.split(cmd))
    url = "http://0.0.0.0:4443"
    timeout = 10
    while True:
        try:
            r = requests.get(url + "/storage/v1/b")
            if r.ok:
                yield url
                break
        except Exception as e:  # noqa: E722
            timeout -= 1
            if timeout < 0:
                raise SystemError from e
            time.sleep(1)
    stop_docker(container)


@pytest.fixture(scope="session")
def gcs_factory(docker_gcs):
    params["endpoint_url"] = docker_gcs

    def factory(**kwargs):
        GCSFileSystem.clear_instance_cache()
        return fsspec.filesystem("gcs", **params, **kwargs)

    return factory


@pytest.fixture(scope="session")
def buckets_to_delete():
    """
    Provides a session-scoped set to track the names of GCS buckets that are
    created by the test suite.

    When tests run, they may create new GCS buckets. If these buckets are not
    deleted, they will persist after the test run, leading to resource leakage.
    This set acts as a registry of buckets that the `final_cleanup` fixture
    should remove at the end of the entire test session.
    """
    return set()


@pytest.fixture
def gcs(gcs_factory, buckets_to_delete, populate=True):
    gcs = gcs_factory()
    try:  # ensure we're empty.
        # Create the bucket if it doesn't exist, otherwise clean it.
        if not gcs.exists(TEST_BUCKET):
            gcs.mkdir(TEST_BUCKET)
            # By adding the bucket name to this set, we are marking it for
            # deletion at the end of the test session. This ensures that if
            # the test suite creates the bucket, it will also be responsible
            # for deleting it. If the bucket already existed, we assume it's
            # managed externally and should not be deleted by the tests.
            buckets_to_delete.add(TEST_BUCKET)
        else:
            try:
                gcs.rm(gcs.find(TEST_BUCKET))
            except Exception as e:
                logging.warning(f"Failed to empty bucket {TEST_BUCKET}: {e}")

        if populate:
            gcs.pipe({TEST_BUCKET + "/" + k: v for k, v in allfiles.items()})
        gcs.invalidate_cache()
        yield gcs
    finally:
        _cleanup_gcs(gcs)


@pytest.fixture
def extended_gcs_factory(gcs_factory, buckets_to_delete, populate=True):
    created_instances = []

    def factory(**kwargs):
        fs = _create_extended_gcsfs(gcs_factory, buckets_to_delete, populate, **kwargs)
        created_instances.append(fs)
        return fs

    yield factory

    for fs in created_instances:
        _cleanup_gcs(fs)


@pytest.fixture
def extended_gcsfs(gcs_factory, buckets_to_delete, populate=True):
    extended_gcsfs = _create_extended_gcsfs(gcs_factory, buckets_to_delete, populate)
    try:
        yield extended_gcsfs
    finally:
        _cleanup_gcs(extended_gcsfs)


def _cleanup_gcs(gcs):
    """Clean the bucket contents, logging a warning on failure."""
    try:
        gcs.rm(gcs.find(TEST_BUCKET))
    except Exception as e:
        logging.warning(f"Failed to clean up GCS bucket {TEST_BUCKET}: {e}")


@pytest.fixture(scope="session", autouse=True)
def final_cleanup(gcs_factory, buckets_to_delete):
    """
    A session-scoped, auto-use fixture that deletes all buckets registered
    in the `buckets_to_delete` set after the entire test session is complete.
    """
    yield
    # This code runs after the entire test session finishes
    use_extended_gcs = os.getenv(
        "GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false"
    ).lower() in (
        "true",
        "1",
    )

    if use_extended_gcs:
        is_real_gcs = (
            os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
        )
        # Mock authentication if not using a real GCS endpoint,
        # since grpc client in extended_gcsfs does not work with anon access
        mock_authentication_manager = (
            patch("google.auth.default", return_value=(None, "fake-project"))
            if not is_real_gcs
            else nullcontext()
        )
    else:
        mock_authentication_manager = nullcontext()

    with mock_authentication_manager:
        gcs = gcs_factory()
        for bucket in buckets_to_delete:
            # The cleanup logic attempts to delete every bucket that was
            # added to the set during the session. For real GCS, only delete if
            # created by the test suite.
            try:
                if gcs.exists(bucket):
                    gcs.rm(bucket, recursive=True)
                    logging.info(f"Cleaned up bucket: {bucket}")
            except Exception as e:
                logging.warning(
                    f"Failed to perform final cleanup for bucket {bucket}: {e}"
                )


@pytest.fixture
def gcs_versioned(gcs_factory, buckets_to_delete):
    gcs = gcs_factory()
    gcs.version_aware = True
    is_real_gcs = (
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
    )
    try:  # ensure we're empty.
        # The versioned bucket might be created by `is_versioning_enabled`
        # in test_core_versioned.py. We must register it for cleanup only if
        # it was created by this test run.
        try:
            from gcsfs.tests.test_core_versioned import (
                _VERSIONED_BUCKET_CREATED_BY_TESTS,
            )

            if _VERSIONED_BUCKET_CREATED_BY_TESTS:
                buckets_to_delete.add(TEST_VERSIONED_BUCKET)
        except ImportError:
            pass  # test_core_versioned is not being run
        if is_real_gcs:
            cleanup_versioned_bucket(gcs, TEST_VERSIONED_BUCKET)
        else:
            # For emulators, we delete and recreate the bucket for a clean state
            try:
                gcs.rm(TEST_VERSIONED_BUCKET, recursive=True)
            except FileNotFoundError:
                pass
            gcs.mkdir(TEST_VERSIONED_BUCKET, enable_versioning=True)
            buckets_to_delete.add(TEST_VERSIONED_BUCKET)
        gcs.invalidate_cache()
        yield gcs
    finally:
        # Ensure the bucket is empty after the test.
        try:
            if is_real_gcs:
                cleanup_versioned_bucket(gcs, TEST_VERSIONED_BUCKET)
        except Exception as e:
            logging.warning(
                f"Failed to clean up versioned bucket {TEST_VERSIONED_BUCKET} after test: {e}"
            )


def cleanup_versioned_bucket(gcs, bucket_name, prefix=None):
    """
    Deletes all object versions in a bucket using the google-cloud-storage client,
    ensuring it uses the same credentials as the gcsfs instance.
    """
    # Define a retry policy for API calls to handle rate limiting.
    # This can retry on 429 Too Many Requests errors, which can happen
    # when deleting many object versions quickly.
    from google.api_core.retry import Retry

    retry_policy = Retry(
        initial=1.0,  # Initial delay in seconds
        maximum=30.0,  # Maximum delay in seconds
        multiplier=1.2,  # Backoff factor
    )

    client = storage.Client(
        credentials=gcs.credentials.credentials, project=gcs.project
    )

    # List all blobs, including old versions
    blobs_to_delete = list(client.list_blobs(bucket_name, versions=True, prefix=prefix))

    if not blobs_to_delete:
        logging.info("No object versions to delete in %s.", bucket_name)
        return

    logging.info(
        "Deleting %d object versions from %s.", len(blobs_to_delete), bucket_name
    )
    time.sleep(2)
    for blob in blobs_to_delete:
        blob.delete(retry=retry_policy)

    logging.info("Successfully deleted %d object versions.", len(blobs_to_delete))


def _create_extended_gcsfs(gcs_factory, buckets_to_delete, populate=True, **kwargs):
    is_real_gcs = (
        os.environ.get("STORAGE_EMULATOR_HOST") == "https://storage.googleapis.com"
    )

    # Mock authentication if not using a real GCS endpoint,
    # since grpc client in extended_gcsfs does not work with anon access
    mock_authentication_manager = (
        patch("google.auth.default", return_value=(None, "fake-project"))
        if not is_real_gcs
        else nullcontext()
    )

    with mock_authentication_manager:
        extended_gcsfs = gcs_factory(**kwargs)
        # Only create/delete/populate the bucket if we are NOT using the real GCS endpoint
        if not is_real_gcs:
            try:
                extended_gcsfs.rm(TEST_ZONAL_BUCKET, recursive=True)
            except FileNotFoundError:
                pass
            extended_gcsfs.mkdir(TEST_ZONAL_BUCKET)
            buckets_to_delete.add(TEST_ZONAL_BUCKET)
            if populate:
                extended_gcsfs.pipe(
                    {TEST_ZONAL_BUCKET + "/" + k: v for k, v in allfiles.items()}
                )
        extended_gcsfs.invalidate_cache()
        return extended_gcsfs


# =============================================================================
# Benchmark Fixtures and Hooks
# =============================================================================


@pytest.fixture
def gcsfs_benchmark_read_write(extended_gcs_factory, request):
    """
    A fixture that creates temporary files for a benchmark run and cleans
    them up afterward.

    It uses the `BenchmarkParameters` object from the test's parametrization
    to determine how many files to create and of what size.
    """
    params = request.param
    gcs = extended_gcs_factory(block_size=params.block_size_bytes)

    prefix = f"{params.bucket_name}/benchmark-files-{uuid.uuid4()}"
    file_paths = [f"{prefix}/file_{i}" for i in range(params.num_files)]

    logging.info(
        f"Setting up benchmark '{params.name}': creating {params.num_files} file(s) "
        f"of size {params.file_size_bytes / 1024 / 1024:.2f} MB each."
    )

    # Define a 16MB chunk size for writing
    chunk_size = 16 * 1024 * 1024
    chunks_to_write = params.file_size_bytes // chunk_size
    remainder = params.file_size_bytes % chunk_size

    # Create files by writing random chunks to avoid high memory usage
    for path in file_paths:
        logging.info(f"Creating file {path}.")
        with gcs.open(path, "wb") as f:
            for _ in range(chunks_to_write):
                f.write(os.urandom(chunk_size))
            if remainder > 0:
                f.write(os.urandom(remainder))

    yield gcs, file_paths, params

    # --- Teardown ---
    logging.info(f"Tearing down benchmark '{params.name}': deleting files.")
    try:
        gcs.rm(prefix, recursive=True)
    except Exception as e:
        logging.error(f"Failed to clean up benchmark files: {e}")


def pytest_benchmark_generate_json(config, benchmarks, machine_info, commit_info):
    """
    Hook to post-process benchmark results before generating the JSON report.
    """
    for bench in benchmarks:
        if "timings" in bench.get("extra_info", {}):
            bench.stats.data = bench.extra_info["timings"]
            bench.stats.min = bench.extra_info["min_time"]
            bench.stats.max = bench.extra_info["max_time"]
            bench.stats.mean = bench.extra_info["mean_time"]
            bench.stats.median = bench.extra_info["median_time"]
            bench.stats.stddev = bench.extra_info["stddev_time"]
            bench.stats.rounds = bench.extra_info["rounds"]

            del bench.extra_info["timings"]
            del bench.extra_info["min_time"]
            del bench.extra_info["max_time"]
            del bench.extra_info["mean_time"]
            del bench.extra_info["median_time"]
            del bench.extra_info["stddev_time"]
            del bench.extra_info["rounds"]
