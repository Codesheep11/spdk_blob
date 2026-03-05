import asyncio
import logging
import math
import threading
from queue import Queue, Empty
from typing import List

import spdk_blob  # The C extension
from .definitions import (
    INITIAL_BLOB_COUNT, MAX_TOTAL_BLOB_COUNT, 
    BLOB_REPOPULATE_BATCH_SIZE, LOW_WATER_MARK
)
from .async_bridge import bridge

logger = logging.getLogger(__name__)

class BlobHandlePool:
    def __init__(self):
        self.blob_size_bytes = -1
        self.blob_size_clusters = 0
        self._free_queue: Queue[int] = Queue()
        self._all_ids: List[int] = []
        self._all_handles: List[int] = []
        
        self._is_repopulating = False
        self._repop_lock = threading.Lock()
        self._initialized = False

    def set_config(self, blob_size_bytes: int):
        if self._initialized:
             raise RuntimeError("Cannot set config after initialization")
        self.blob_size_bytes = blob_size_bytes

    def init_pool(self, cluster_size: int):
        if self._initialized: return
        if self.blob_size_bytes <= 0:
             raise RuntimeError("Blob size not configured.")

        self.blob_size_clusters = math.ceil(self.blob_size_bytes / cluster_size)
        logger.info(f"Initializing pool with {INITIAL_BLOB_COUNT} blobs...")

        # Initial synchronous population
        try:
            new_ids = [
                bridge.submit_blocking(spdk_blob.create_async, self.blob_size_clusters)
                for _ in range(INITIAL_BLOB_COUNT)
            ]
            new_handles = [
                bridge.submit_blocking(spdk_blob.open_async, bid)
                for bid in new_ids
            ]
            
            with self._repop_lock:
                self._all_ids.extend(new_ids)
                self._all_handles.extend(new_handles)
                for h in new_handles:
                    self._free_queue.put(h)
            
            self._initialized = True
            logger.info("Blob pool initialized.")
        except Exception as e:
            logger.error("Pool init failed", exc_info=True)
            raise e
    
    def recover_and_init_pool(self, used_blob_ids: List[int], cluster_size: int):
        if self._initialized:
            logger.warning("Pool already initialized, skipping recovery.")
            return

        if self.blob_size_bytes <= 0:
            raise RuntimeError("Blob size not configured.")

        self.blob_size_clusters = math.ceil(self.blob_size_bytes / cluster_size)
        
        logger.info(f"Recovering {len(used_blob_ids)} blobs from persistence...")

        try:
            # 1. 打开旧的 Blob (Used Blobs)
            # 注意：这些 Blob 不会放入 _free_queue，因为它们属于“占用”状态
            used_handles = []
            if used_blob_ids:
                # 必须分批处理，避免一次性提交过多 C 请求
                batch_size = 128
                for i in range(0, len(used_blob_ids), batch_size):
                    batch_ids = used_blob_ids[i : i + batch_size]
                    futures = [
                        bridge.submit_async(spdk_blob.open_async, bid) 
                        for bid in batch_ids
                    ]
                    batch_handles = bridge.submit_blocking(lambda *args: asyncio.gather(*futures), *args) # Hacky call to run asyncio.gather in bridge

                used_handles = [
                    bridge.submit_blocking(spdk_blob.open_async, bid)
                    for bid in used_blob_ids
                ]

            # 2. 补齐剩余的 Blob (Free Blobs)
            current_count = len(used_blob_ids)
            blobs_needed = max(0, INITIAL_BLOB_COUNT - current_count)
            
            logger.info(f"Restored {current_count} used blobs. Creating {blobs_needed} new blobs to fill pool...")

            new_ids = []
            new_handles = []
            if blobs_needed > 0:
                new_ids = [
                    bridge.submit_blocking(spdk_blob.create_async, self.blob_size_clusters)
                    for _ in range(blobs_needed)
                ]
                new_handles = [
                    bridge.submit_blocking(spdk_blob.open_async, bid)
                    for bid in new_ids
                ]

            # 3. 更新内部状态
            with self._repop_lock:
                self._all_ids.extend(used_blob_ids)
                self._all_ids.extend(new_ids)
                
                self._all_handles.extend(used_handles)
                self._all_handles.extend(new_handles)
                
                # 只有新创建的 blobs 放入 free queue
                for h in new_handles:
                    self._free_queue.put(h)

            self._initialized = True
            logger.info("Blob pool recovery and initialization complete.")
            
            # 返回 ID -> Handle 的映射，供 Backend 重建 Metadata
            return dict(zip(used_blob_ids, used_handles))

        except Exception as e:
            logger.error("Pool recovery failed", exc_info=True)
            self.teardown()
            raise e

    def teardown(self):
        logger.info("Cleaning up blob pool...")
        try:
            for handle in self._all_handles:
                bridge.submit_blocking(spdk_blob.close_async, handle)
            for bid in self._all_ids:
                bridge.submit_blocking(spdk_blob.delete_async, bid)
        except Exception as e:
            logger.warning(f"Error during cleanup: {e}")
        
        self._all_handles.clear()
        self._all_ids.clear()
        # Drain queue
        while not self._free_queue.empty():
            try: self._free_queue.get_nowait()
            except Empty: break
        self._initialized = False

    def get_blob(self, timeout: float = 5.0) -> int:
        try:
            handle = self._free_queue.get(timeout=timeout)
            if self._free_queue.qsize() < LOW_WATER_MARK:
                self._trigger_repopulation()
            return handle
        except Empty:
            raise RuntimeError("Blob pool empty and repopulation failed/slow.")

    def release_blob(self, handle: int):
        self._free_queue.put(handle)

    def _trigger_repopulation(self):
        with self._repop_lock:
            if self._is_repopulating: return
            if len(self._all_handles) >= MAX_TOTAL_BLOB_COUNT: return
            self._is_repopulating = True
        
        logger.info("Triggering async blob repopulation.")
        bridge.run_coroutine(self._repopulate_task())

    async def _repopulate_task(self):
        try:
            with self._repop_lock:
                current = len(self._all_handles)
                to_create = min(BLOB_REPOPULATE_BATCH_SIZE, MAX_TOTAL_BLOB_COUNT - current)
            
            if to_create <= 0: return

            logger.debug(f"Repopulating {to_create} blobs...")
            
            # Create
            create_futures = [
                bridge.submit_async(spdk_blob.create_async, self.blob_size_clusters) 
                for _ in range(to_create)
            ]
            new_ids = await asyncio.gather(*create_futures)

            # Open
            open_futures = [
                bridge.submit_async(spdk_blob.open_async, bid) 
                for bid in new_ids
            ]
            new_handles = await asyncio.gather(*open_futures)

            with self._repop_lock:
                self._all_ids.extend(new_ids)
                self._all_handles.extend(new_handles)
                for h in new_handles:
                    self._free_queue.put(h)
            
            logger.info(f"Pool repopulated. Total: {len(self._all_handles)}")

        except Exception as e:
            logger.error(f"Repopulation failed: {e}", exc_info=True)
        finally:
            with self._repop_lock:
                self._is_repopulating = False

# Global Instance
blob_pool = BlobHandlePool()