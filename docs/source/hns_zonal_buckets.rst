Rapid Storage (Zonal Buckets) and Hierarchical Namespace (HNS)
==============================================

To train, checkpoint, and serve AI models at peak efficiency, Google Cloud Storage (GCS) offers **Rapid Storage** and **Hierarchical Namespace (HNS)**.

``gcsfs`` provides opt-in support for operations on both HNS and Zonal buckets.

Enabling HNS and Zonal Support
------------------------------

Support for Rapid storage and HNS is currently opt-in. You can enable these features by explicitly passing the ``EXPERIMENTAL_ZONAL_HNS=True`` flag when instantiating the filesystem, or by setting an environment variable of the same name.

**Option 1: Via Keyword Argument**

.. code-block:: python

    import gcsfs

    fs = gcsfs.GCSFileSystem(EXPERIMENTAL_ZONAL_HNS=True)

**Option 2: Via Environment Variable**

If you are running jobs on a cluster or don't want to modify your Python code, you can enable it globally via your environment:

.. code-block:: bash

    export EXPERIMENTAL_ZONAL_HNS=true

What is Rapid Storage?
-----------------------
Standard GCS buckets span multiple zones within a region or across multiple regions. `Rapid Storage <https://cloud.google.com/blog/products/storage-data-transfer/high-performance-storage-innovations-for-ai-hpc#:~:text=Rapid%20Storage%20enables%20AI%20workloads%20with%20millisecond%2Dlatency>`_ localize your data and metadata to a single Google Cloud zone (e.g., ``us-central1-a``).

* **Low Latency & Rapid Access:** Designed for the Rapid access tier, co-locating your compute resources (like GKE clusters, Dataproc, or Compute Engine VMs) in the exact same zone as your Zonal bucket significantly reduces storage access latency and avoids cross-zone egress.
* **High Performance:** Architected specifically to feed data-hungry applications like AI/ML training accelerators (TPUs/GPUs) without storage bottlenecks.

*Note: Rapid Storage buckets currently have HNS enabled by default.*

What is a Hierarchical Namespace (HNS)?
---------------------------------------

Historically, GCS buckets have utilized a **flat namespace**. In a flat namespace, directories do not exist as distinct physical entities; they are simulated by 0-byte objects ending in a slash (``/``) or by filtering object prefixes during list operations.

A `Hierarchical Namespace (HNS) <https://cloud.google.com/storage/docs/hns-overview>`_ introduces true, logical directories as first-class resources to GCS.

Usage Example
-------------

Ensure your compute VM or Kubernetes Pod is running in the same zone as the Rapid Storage bucket to fully realize the latency benefits.

.. code-block:: python

    import gcsfs
    import pandas as pd

    # 1. Enable the opt-in flag
    fs = gcsfs.GCSFileSystem(EXPERIMENTAL_ZONAL_HNS=True)

    # 2. Perform instant directory renames (O(1) operation)
    # In standard GCS, this could take minutes for large datasets.
    # In HNS GCS, this is instantaneous.
    fs.rename('my-zonal-bucket/staging_data/', 'my-zonal-bucket/production_data/')

    # 3. Read data seamlessly
    with fs.open('my-zonal-bucket/production_data/dataset.parquet', 'rb') as f:
        df = pd.read_parquet(f)

Important Differences to Keep in Mind
-------------------------------------
While ``gcsfs`` aims to abstract the differences via the ``fsspec`` API, you should be aware of standard HNS and Rapid storage limitations imposed by the Google Cloud Storage API:

1. **Transactions are not supported with Rapid Storage**

For more details on managing these buckets, refer to the official documentation for `Hierarchical Namespace <https://cloud.google.com/storage/docs/hns-overview>`_ and `Zonal Storage <https://cloud.google.com/storage/docs/zonal-buckets>`_.
