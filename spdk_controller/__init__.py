from .core import engine
from .pool import blob_pool
from .definitions import C_IO_Request
import spdk_blob 
import logging

logger = logging.getLogger(__name__)

# --- Lifecycle API ---
def init(bdev_name: str, json_config_path: str, reactor_mask: str, main_core: int, sock: str) -> None:
    engine.init(bdev_name, json_config_path, reactor_mask, main_core, sock)

def load(bdev_name: str, json_config_path: str, reactor_mask: str, main_core: int, sock: str) -> None:
    engine.load(bdev_name, json_config_path, reactor_mask, main_core, sock)

def unload() -> None:
    engine.unload()

# --- Config API ---
def set_blob_size_in_bytes(size_in_bytes: int) -> None:
    blob_pool.set_config(size_in_bytes)

def recover_and_init_pool(used_blob_ids):
    if not engine.initialized:
        raise RuntimeError("SPDK must be loaded before recovering pool.")
    cluster_size = engine.get_cluster_size()
    return blob_pool.recover_and_init_pool(used_blob_ids, cluster_size)

def init_blob_pool() -> None:
    if not engine.initialized:
        raise RuntimeError("SPDK must be initialized before initializing pool.")
    cluster_size = engine.get_cluster_size()
    blob_pool.init_pool(cluster_size)

def get_blob_size_in_bytes() -> int:
    return blob_pool.blob_size_bytes

# --- Blob Management API ---
def get_blob(timeout: float = 5.0) -> int:
    return blob_pool.get_blob(timeout)

def release_blob(handle: int):
    blob_pool.release_blob(handle)

def get_blob_id(handle: int) -> int:
    return spdk_blob.get_blob_id(handle)

# --- IO API ---
def write_batch_async(requests):
    return engine.write_batch_async(requests)

def read_batch_async(requests):
    return engine.read_batch_async(requests)

# Backward compatibility wrappers
def write_async(handle, ptr, offset, size):
    return engine.write_batch_async([(handle, ptr, offset, size)])

def read_async(handle, ptr, offset, size):
    return engine.read_batch_async([(handle, ptr, offset, size)])

# --- Memory API ---
def alloc_io_buffer_view(size: int):
    return spdk_blob.alloc_io_buffer_view(size)

def free_io_buffer(ptr: int):
    spdk_blob.free_io_buffer(ptr)

# --- Metadata API ---
def get_page_size(): return engine.get_page_size()
def get_cluster_size(): return engine.get_cluster_size()
def get_io_unit_size(): return engine.get_io_unit_size()