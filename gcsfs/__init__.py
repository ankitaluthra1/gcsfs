import os
from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
from .core import GCSFileSystem
from .mapping import GCSMap
if os.getenv("GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT", "false").lower() in ("true", "1"):
    try:
        import gcsfs.extended_gcsfs
        print("INFO: gcsfs experimental features enabled via GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT.")
    except ImportError as e:
        print(f"WARNING: GCSFS_EXPERIMENTAL_ZB_HNS_SUPPORT is set, but failed to import experimental features: {e}")

__all__ = ["GCSFileSystem", "GCSMap"]

from . import _version

__version__ = _version.get_versions()["version"]
