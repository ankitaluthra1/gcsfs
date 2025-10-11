import asyncio
from io import BytesIO

from .core import GCSFileSystem

test_project = "vaibhavpratap-sdk-test"
object_path = "chandrasiri-rs/sunidhi"

fs_zonal = GCSFileSystem(project = test_project, experimental_zb_hns_support=True)
f= fs_zonal.open(object_path, 'rb') 
f.seek(9)
chunk = f.read(100)
print("Chunk: ", chunk)