=============================
Rapid Storage (Zonal Buckets)
=============================

To accelerate data-intensive workloads such as AI/ML training, model checkpointing, and analytics, Google Cloud Storage (GCS) offers **Rapid Storage** through **Zonal Buckets**.

``gcsfs`` provides full support for accessing, reading, and writing to Rapid Storage buckets.

What is Rapid Storage?
----------------------

Rapid Storage is a storage class designed for **high-performance data access** in Cloud Storage.
Unlike standard buckets that span an entire region or multi-region, Rapid buckets are **Zonal**—they are located within a specific Google Cloud zone. This allows you to co-locate your storage with your compute resources (such as GPUs or TPUs),
resulting in significantly lower latency and higher throughput.

Key capabilities include:

* **Low latency and high throughput:** Ideal for data-intensive AI/ML, evaluating models, and logging.
* **Native Appends:** You can natively append data to objects.
* **Immediate Visibility:** Appendable objects appear in the bucket namespace as soon as you start writing to them and can be read concurrently.

You can find detailed documentation on zonal bucket here: https://docs.cloud.google.com/storage/docs/rapid/rapid-bucket.

Using Rapid Storage with ``gcsfs``
----------------------------------
Rapid Storage is fully supported by gcsfs without any code changes needed .To interact with Rapid Storage,
the underlying filesystem operations will automatically route through
the newly added ``ExtendedFileSystem`` designed to support multiple storage types like HNS and Rapid.
You can interact with these buckets just like any other filesystem.

**Code Example**

.. code-block:: python

    import gcsfs

    # Initialize the filesystem
    fs = gcsfs.GCSFileSystem()

    # Writing to a Rapid bucket
    with fs.open('my-zonal-rapid-bucket/data/checkpoint.pt', 'wb') as f:
        f.write(b"model data...")

    # Appending to an existing object (Native Rapid feature)
    with fs.open('my-zonal-rapid-bucket/data/checkpoint.pt', 'ab') as f:
        f.write(b"appended data...")

Under the Hood: The ``ExtendedFileSystem`` and ``ZonalFile``
------------------------------------------------------------

`gcsfs` enables Rapid Storage support through the ``ExtendedFileSystem`` and a specialized ``ZonalFile`` file handler. Both ``ExtendedFileSystem`` and ``ZonalFile`` inherits same semantics as existing ``GCSFileSystem`` and ``GCSFile``
making Rapid support fully backward compatible for all operations.

At initialization, ``ExtendedFileSystem`` evaluates the underlying bucket's storage layout. If it detects Rapid storage, file-level operations are dynamically routed to the ``ZonalFile`` class instead of the standard ``GCSFile``.

Unlike standard operations which use HTTP endpoints, ``ZonalFile`` utilizes the Google Cloud Storage gRPC API—specifically the ``AsyncMultiRangeDownloader`` (MRD) for reads and ``AsyncAppendableObjectWriter`` (AAOW) for writes.

Operation Semantics: Standard vs. Rapid Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The table below highlights how core filesystem and file-level operations change when interacting with a Rapid Zonal bucket compared to a standard GCS bucket.

.. list-table::
   :widths: 15 40 45
   :header-rows: 1

   * - Class / Method
     - Standard Storage (``GCSFileSystem`` / ``GCSFile``)
     - Rapid Storage (``ExtendedFileSystem`` / ``ZonalFile``)
   * - **open(mode='a')**
     - **Not supported.** Overwritten to ``w`` mode with a warning.
     - **Supported.** Natively opens an append stream to the object via gRPC.
   * - **ExtendedFileSystem._open**
     - Returns a standard ``GCSFile`` instance.
     - Returns a ``ZonalFile`` instance, initializing gRPC streams.
   * - **cat_file`` / ``_fetch_range**
     - Uses standard HTTP GET range requests.
     - Uses gRPC `AsyncMultiRangeDownloader <https://github.com/googleapis/python-storage/blob/8b7fbde10c80337c4b4a2f6c8a860e28371a770b/google/cloud/storage/asyncio/async_multi_range_downloader.py#L92>`_ (MRD) for parallel byte-range fetching.
   * - **get_file**
     - Downloads using standard HTTP GET requests.
     - Downloads via gRPC MRD in configurable chunks.
   * - **put_file / pipe_file**
     - Uses HTTP multipart or resumable uploads.
     - Uses Bidirectional RPC (`AsyncAppendableObjectWriter <https://github.com/googleapis/python-storage/blob/8b7fbde10c80337c4b4a2f6c8a860e28371a770b/google/cloud/storage/asyncio/async_appendable_object_writer.py#L102>`_) for direct, high-performance writes.
   * - **cp_file (Copy)**
     - Server-side rewrite (``rewriteTo`` API).
     - **Not supported.** Raises ``NotImplementedError`` as zonal objects do not support rewrites.
   * - **write / flush**
     - Buffers locally and uploads chunks via HTTP POST when flushed.
     - Streams chunks directly to the AAOW stream for immediate persistence.
   * - **discard**
     - Cancels an in-progress HTTP multi-upload and cleans up.
     - **Not applicable.** Logs a warning since streaming data cannot be canceled.
   * - **close**
     - Finalizes the file upload to GCS.
     - Closes streams but leaves the object unfinalized (appendable) by default, unless ``finalize_on_close=True`` or `.commit()` is done.
   * - **mv**
     - Object-level copy-and-delete logic.
     - Uses native, atomic ``rename_folder`` API for folders. All directory semantics described in the :doc:`HNS documentation <hns_buckets>` also apply For Rapid.

Performance Benchmarks
----------------------

Rapid Storage via gRPC significantly improves read and write performance compared to standard HTTP regional buckets.
Here are the microbenchmarks
Rapid drastically outperform standard buckets across different read patterns, including both sequential and random reads, as well as for writes.
To reproduce using more combinations, please see `gcsfs/perf/micrbenchmarks`

.. list-table:: **Sequential Reads**
   :header-rows: 1

   * - IO Size
     - Streams
     - Rapid (MB/s)
     - Standard (MB/s)
   * - 1 MB
     - Single Stream
     - 469.09
     - 37.76
   * - 16 MB
     - Single Stream
     - 628.59
     - 64.50
   * - 1 MB
     - 48 Streams
     - 16932
     - 2202
   * - 16 MB
     - 48 Streams
     - 19213.27
     - 4010.50

.. list-table:: **Random Reads**
   :header-rows: 1

   * - IO Size
     - Streams
     - Rapid Throughput (MB/s)
     - Standard (MB/s)
   * - 64 KB
     - Single Stream
     - 39
     - 0.77
   * - 16 MB
     - Single Stream
     - 602.12
     - 66.92
   * - 64 KB
     - 48 Streams
     - 2081
     - 51
   * - 16 MB
     - 48 Streams
     - 21448
     - 4504

.. list-table:: **Writes**
   :widths: 20 20 30 30
   :header-rows: 1

   * - IO Size
     - Streams
     - Zonal Bucket
     - Regional Bucket
   * - 16 MB
     - Single Stream
     - 326
     - 100
   * - 16 MB
     - 48 Streams
     - 13418
     - 4722

Multiprocessing and gRPC
----------------------------------------------

Because `gcsfs` relies on gRPC to interact with Rapid storage, developers must be careful when using multiprocessing. Users use libraries such as multiprocessing, subprocess, concurrent.futures.ProcessPoolExecutor, etc, to work around the GIL. These modules call `fork()` underneath the hood.

However, gRPC Python wraps gRPC core, which uses internal multithreading for performance, and hence doesn't support `fork()`.
Using `fork()` for multi-processing can lead to hangs or segmentation faults when child processes attempt to use the network layer.

**The Fix: Use `spawn` instead of `fork`**

To resolve this, the safest option will be to use `spawn` instead of `fork` where the child process will create their own grpc connection.

You can configure Python's `multiprocessing` module to use the `spawn` start method as shown in the snippet below.
For example while using data loaders in frameworks like PyTorch
(e.g., `torch.utils.data.DataLoader` with `num_workers > 0`) alongside `gcsfs` with Rapid storage, you should add `spawn` as start method:

.. code-block:: python

    import torch.multiprocessing
    # This must be done before other imports or initialization
    try:
      torch.multiprocessing.set_start_method('spawn', force=True)
    except RuntimeError:
      pass # Context already set

Important Differences to Keep in Mind
-------------------------------------

When working with Rapid Storage in ``gcsfs``, keep the following GCS limitations and behaviors in mind:

1. **HNS Requirement:** You cannot use Rapid Storage without a Hierarchical Namespace. All directory semantics described in the :doc:`HNS documentation <hns_buckets>` apply here (e.g., real folder resources, strict ``mkdir`` behavior).
2. **Append Semantics:** In a standard flat GCS bucket, appending to a file is typically a costly operation requiring a full object download and rewrite. Rapid Storage supports **native appends**. When you open a file in append mode (``ab``), ``gcsfs`` natively appends to the object and the object's size grows in real-time.
3. **Single Writer:** Appendable objects can only have one active writer at a time. If a new write stream is established for an object, the original stream is interrupted and will return an error from Cloud Storage.
4. **Finalization:** Once an object is finalized (e.g., the write stream is closed), you can no longer append to it. To take advantage of `native appends`, gcsfs keeps the object unfinalized by default.
5. **Incompatibilities:** Zonal buckets currently do not support certain standard GCS features, full list is here: https://docs.cloud.google.com/storage/docs/rapid/rapid-bucket#incompatibilities


For more details on managing, pricing, and optimizing these buckets, refer to the official documentation for `Rapid Bucket <https://cloud.google.com/storage/docs/rapid/rapid-bucket>`_.
