import os
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
from .core import GCSFileSystem
from .mapping import GCSMap
if os.getenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false").lower() in ("true", "1"):
    try:
        from .extended_gcsfs import ExtendedGcsFileSystem
        print("INFO: gcsfs experimental features enabled via GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT.")
        # Rebind the name GCSFileSystem to the extended version
        GCSFileSystem = ExtendedGcsFileSystem
        # # Explicitly register the extended version, overwriting any default
        # register_implementation("gs", GCSFileSystem, clobber=True)
        # register_implementation("gcs", GCSFileSystem, clobber=True)
    except ImportError as e:
        print(f"WARNING: GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT is set, but failed to import experimental features: {e}")
        # Fallback to core GCSFileSystem, do not register here

__all__ = ["GCSFileSystem", "GCSMap"]

from . import _version

__version__ = _version.get_versions()["version"]
