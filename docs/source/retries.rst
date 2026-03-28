Robust Retries in GCSFS
=======================

To ensure resilience against transient network failures and Google Cloud Storage (GCS) server-side drops, ``gcsfs`` implements a robust, time-bound, and idempotent retry strategy across different bucket types (standard, zonal, and Hierarchical Namespace).

This documentation covers the retry logic applied to different methods and bucket configurations.

Standard (Flat Namespace) Buckets
--------------------------------

For standard flat-namespace buckets, ``gcsfs`` relies on a default retry decorator ``retry_request`` (defined in ``gcsfs/retry.py``) for most operations.

This engine enforces:

1. **Max Retries**: Defaults to 6 attempts (1 initial + 5 retries).
2. **Exponential Backoff**: Resurfaces transient HTTP errors (e.g., 408, 429, 5xx) and retries with exponential backoff (``min(random.random() + 2**(retry-1), 32)``).
3. **Exceptions Retried**:
   - ``HttpError`` (for specific retriable status codes)
   - ``requests.exceptions.RequestException``
   - ``google.auth.exceptions.GoogleAuthError``
   - ``ChecksumError``
   - ``aiohttp.client_exceptions.ClientError``

This default retry logic applies to standard file operations (e.g., ``cat``, ``put``, ``cp``, ``rm`` on standard buckets).

Batch Operations
----------------

For multi-object deletions (batch deletion), GCSFS uses a custom retry loop that processes up to 5 attempts (1 initial + 4 retries) with exponential backoff and jitter. This logic removes successfully deleted objects from subsequent attempts and focuses retries only on failed selections.

Specialized Buckets (Zonal and Hierarchical Namespace)
------------------------------------------------------

For method calls routed to the **GCS Storage Control API** (applicable strictly to HNS and Zonal buckets), ``gcsfs`` utilizes a custom asynchronous retry wrapper: ``execute_with_timebound_retry``.

This engine enforces several failsafe constraints:

1. **Strict Per-Attempt Timeout**: Every individual gRPC call is bounded by a strict timeout (configured via ``retry_deadline``, defaulting to 30.0s). If the server fails to respond within this threshold, `asyncio.wait_for` forcefully cancels the stalled iteration.
2. **Grace Window**: A persistent 1.0-second grace window is attached to each attempt (yielding the actual timeout applied slightly larger, at ``retry_deadline + 1.0``). This provides sufficient time for native gRPC transport errors to surface accurately before localized client-side thresholds fire.
3. **Count-Bounded Mapping**: The retry loop strictly enforces a maximum attempt cap of exactly 6 (1 initial + 5 fallback retries). After hitting this precise threshold, client errors are propagated directly.
4. **Exponential Backoff and Jitter**: Transient gRPC exceptions undergo custom exponential backoff defined by ``min(random.random() + 2**(attempt-1), 32)``.

**Exceptions Retried**:

The following transient gRPC exceptions are retried:

- ``google.api_core.exceptions.RetryError``
- ``google.api_core.exceptions.DeadlineExceeded``
- ``google.api_core.exceptions.ServiceUnavailable``
- ``google.api_core.exceptions.InternalServerError``
- ``google.api_core.exceptions.TooManyRequests``
- ``google.api_core.exceptions.ResourceExhausted``
- ``google.api_core.exceptions.Unknown``
- ``asyncio.TimeoutError``
- ``google.api_core.exceptions.Unauthenticated`` (when the message contains "Invalid Credentials")


Methods Supported by Time-Bound Retries
----------------------------------------

Below is a serialized list of API functions implicitly wrapped by the failsafe engine:

.. list-table:: **Supported Storage Control API Methods**
   :widths: 30 70
   :header-rows: 1

   * - High-Level Method
     - Underlying Storage Control API Call
   * - **``mkdir``**
     - ``client.create_folder``
   * - **``rmdir``** / **``rm``**
     - ``client.delete_folder``
   * - **``mv``** / **``rename``**
     - ``client.rename_folder``
   * - **``info``**
     - ``client.get_folder``
   * - **`_is_bucket_hns_enabled`**
     - ``client.get_storage_layout``
