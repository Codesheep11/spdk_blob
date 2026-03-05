import itertools
import logging
import time
import ctypes
from typing import List, Tuple
from concurrent.futures import Future

import spdk_blob
from .definitions import C_IO_Request
from .async_bridge import bridge
from .pool import blob_pool

logger = logging.getLogger(__name__)

class SpdkCore:
    def __init__(self):
        self.initialized = False
        self.worker_count = 0
        self.worker_cycler = None
        
        # Caches
        self._page_size = -1
        self._cluster_size = -1
        self._io_unit_size = -1

    def _prepare_start(self, bdev_name, json_config_path, reactor_mask, main_core, sock):
        if self.initialized: return False
        
        if main_core < 0:
            raise ValueError(f"main_core must be a non-negative integer, but got {main_core}")
        
        bridge.start()
        return True

    def _finish_start(self):
        # Setup Workers
        self.worker_count = spdk_blob.get_worker_count()
        if self.worker_count <= 0:
            self.unload()
            raise RuntimeError("No workers found.")
        self.worker_cycler = itertools.cycle(range(self.worker_count))

        # Cache Metadata
        self._page_size = spdk_blob.get_page_size()
        self._cluster_size = spdk_blob.get_cluster_size()
        self._io_unit_size = spdk_blob.get_io_unit_size()

        self.initialized = True
        logger.info("SPDK Core started successfully.")

    def init(self, bdev_name: str, json_config_path: str, reactor_mask: str, main_core: int, sock: str):
        """Format and start a new blobstore."""
        if not self._prepare_start(bdev_name, json_config_path, reactor_mask, main_core, sock): return
        
        # Call C init (format)
        spdk_blob.init(bdev_name, json_config_path, reactor_mask, sock, bridge.loop, main_core)
        
        self._finish_start()
        # Note: Pool init will happen later when set_blob_size is called or manually triggered.

    def load(self, bdev_name: str, json_config_path: str, reactor_mask: str, main_core: int, sock: str):
        """Load an existing blobstore."""
        if not self._prepare_start(bdev_name, json_config_path, reactor_mask, main_core, sock): return
        
        # Call C load (restore)
        try:
            spdk_blob.load(bdev_name, json_config_path, reactor_mask, sock, bridge.loop, main_core)
        except OSError as e:
            logger.error("Failed to load SPDK blobstore. The device might not be formatted.")
            bridge.stop()
            raise e
            
        self._finish_start()

    def unload(self):
        if not self.initialized: return
        
        blob_pool.teardown()
        spdk_blob.unload()
        bridge.stop()
        
        self.initialized = False
        self.worker_cycler = None
        logger.info("SPDK Core unloaded.")

    def get_next_worker(self) -> int:
        if not self.worker_cycler:
             raise RuntimeError("SPDK not initialized")
        return next(self.worker_cycler)

    # --- IO Methods ---
    def write_batch_async(self, requests: List[Tuple[int, int, int, int]]) -> Future:
        if not self.initialized: raise RuntimeError("SPDK not initialized")
        if not requests:
            f = Future()
            f.set_result(None)
            return f

        worker_id = self.get_next_worker()
        batch_size = len(requests)
        c_reqs = (C_IO_Request * batch_size)()
        
        io_unit = self._io_unit_size

        for i, (handle, ptr, offset, size) in enumerate(requests):
            c_reqs[i].handle = handle
            c_reqs[i].payload = ptr
            c_reqs[i].offset_units = offset // io_unit
            c_reqs[i].num_units = size // io_unit
        
        future = Future()
        spdk_blob.write_batch_async(worker_id, ctypes.addressof(c_reqs), batch_size, future)
        return future

    def read_batch_async(self, requests: List[Tuple[int, int, int, int]]) -> Future:
        if not self.initialized: raise RuntimeError("SPDK not initialized")
        if not requests:
            f = Future()
            f.set_result(None)
            return f

        worker_id = self.get_next_worker()
        batch_size = len(requests)
        c_reqs = (C_IO_Request * batch_size)()
        
        io_unit = self._io_unit_size

        for i, (handle, ptr, offset, size) in enumerate(requests):
            c_reqs[i].handle = handle
            c_reqs[i].payload = ptr
            c_reqs[i].offset_units = offset // io_unit
            c_reqs[i].num_units = size // io_unit

        future = Future()
        spdk_blob.read_batch_async(worker_id, ctypes.addressof(c_reqs), batch_size, future)
        return future

    # Getters for metadata
    def get_page_size(self): return self._page_size
    def get_cluster_size(self): return self._cluster_size
    def get_io_unit_size(self): return self._io_unit_size

# Global Instance
engine = SpdkCore()