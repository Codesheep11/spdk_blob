import asyncio
import spdk_blob
import math
import logging
import threading
import time
import ctypes
import itertools
from typing import Dict, Tuple, Any, List
from concurrent.futures import Future
from queue import Queue, Empty

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s [%(levelname)s] - %(message)s')

# --- Module Globals ---
_spdk_initialized = False
_BLOB_SIZE_IN_BYTES = 56 * (1024 ** 2)
_INITIAL_BLOB_COUNT = 8192 + 2048
_MAX_TOTAL_BLOB_COUNT = 8192 * 4
_BLOB_REPOPULATE_BATCH_SIZE = _INITIAL_BLOB_COUNT // 4
_LOW_WATER_MARK = _INITIAL_BLOB_COUNT // 4
_completion_loop: asyncio.AbstractEventLoop = None
_completion_thread: threading.Thread = None
_free_blobs_queue: Queue[int] = Queue()

_worker_count = 0
_worker_rr_cycler = None

_all_blob_ids: List[int] = []
_all_blob_handles: List[int] = []
_blob_size_in_clusters = 0
_is_repopulating = False
_repopulation_state_lock = threading.Lock()
_page_size_cache: int = -1
_cluster_size_cache: int = -1
_io_unit_size_cache: int = -1
_metadata_cache_lock = threading.Lock()

class _C_IO_Request(ctypes.Structure):
    _fields_ = [
        ("handle", ctypes.c_void_p),      # 对应 spdk_blob_handle (void*)
        ("payload", ctypes.c_void_p),     # 对应 void*
        ("offset_units", ctypes.c_uint64), # 对应 uint64_t
        ("num_units", ctypes.c_uint64),    # 对应 uint64_t
    ]


# --- Internal Implementation ---
def _completion_loop_runner():
    """Background thread running an asyncio event loop for C callbacks."""
    global _completion_loop
    asyncio.set_event_loop(asyncio.new_event_loop())
    _completion_loop = asyncio.get_event_loop()
    logging.info("Completion event loop started.")
    _completion_loop.run_forever()
    _completion_loop.close()
    logging.info("Completion event loop stopped.")

def _submit_meta_op_blocking(spdk_func, *args) -> Any:
    """Helper to call an async metadata op and block for its result."""
    if not _completion_loop or not _completion_loop.is_running():
        raise RuntimeError("SPDK completion loop is not running.")
    future = Future()
    def on_done(async_future):
        try:
            future.set_result(async_future.result())
        except Exception as e:
            future.set_exception(e)
    def do_submit():
        loop_future = _completion_loop.create_future()
        loop_future.add_done_callback(on_done)
        spdk_func(*args, loop_future)
    _completion_loop.call_soon_threadsafe(do_submit)
    return future.result(timeout=20)

def _get_next_worker_id() -> int:
    """
    通过线程安全的轮询策略获取下一个 worker ID。
    """
    if not _worker_rr_cycler:
        raise RuntimeError("SPDK worker pool is not initialized. Call init() first.")
    return next(_worker_rr_cycler)

async def _submit_meta_op_async(spdk_func, *args) -> Any:
    """Helper to call an async metadata op and await its result in an async context."""
    if not _completion_loop or not _completion_loop.is_running():
        raise RuntimeError("SPDK completion loop is not running.")
    loop_future = _completion_loop.create_future()
    _completion_loop.call_soon_threadsafe(spdk_func, *args, loop_future)
    return await loop_future

async def _repopulate_blobs_task():
    """
    The actual coroutine that creates blobs and adds them to the pool.
    This runs on the _completion_loop.
    """
    global _is_repopulating
    logging.info("Async blob repopulation task started.")
    try:
        with _repopulation_state_lock:
            current_count = len(_all_blob_handles)
            if current_count >= _MAX_TOTAL_BLOB_COUNT:
                logging.warning(f"Max blob count ({_MAX_TOTAL_BLOB_COUNT}) reached. Cannot repopulate.")
                return
            num_to_create = min(_BLOB_REPOPULATE_BATCH_SIZE, _MAX_TOTAL_BLOB_COUNT - current_count)
        if num_to_create <= 0: return
        logging.info(f"Repopulating with a new batch of {num_to_create} blobs...")
        create_tasks = [_submit_meta_op_async(spdk_blob.create_async, _blob_size_in_clusters) for _ in range(num_to_create)]
        new_ids = await asyncio.gather(*create_tasks)
        open_tasks = [_submit_meta_op_async(spdk_blob.open_async, bid) for bid in new_ids]
        new_handles = await asyncio.gather(*open_tasks)
        with _repopulation_state_lock:
             _all_blob_ids.extend(new_ids)
             _all_blob_handles.extend(new_handles)
             for handle in new_handles:
                 _free_blobs_queue.put(handle)
        logging.info(f"Blob pool repopulated. Total blobs now: {len(_all_blob_handles)}")
    except Exception as e:
        logging.error(f"Failed to repopulate blob pool asynchronously: {e}", exc_info=True)
    finally:
        with _repopulation_state_lock:
            _is_repopulating = False
        logging.info("Async blob repopulation task finished.")

def _trigger_repopulation():
    """
    Checks if repopulation is needed and not already running, then schedules the async task.
    This function is thread-safe and non-blocking.
    """
    global _is_repopulating
    with _repopulation_state_lock:
        if _is_repopulating: return
        current_count = len(_all_blob_handles)
        if current_count >= _MAX_TOTAL_BLOB_COUNT: return
        _is_repopulating = True
    logging.info("Low water mark hit. Asynchronous blob repopulation triggered.")
    _completion_loop.call_soon_threadsafe(lambda: _completion_loop.create_task(_repopulate_blobs_task()))


# --- Public API ---
def init(bdev_name: str, json_config_path: str, reactor_mask: str, sock: str) -> None:
    """Initializes the SPDK environment and resources."""
    global _spdk_initialized, _completion_thread, _all_blob_ids, _all_blob_handles
    global _blob_size_in_clusters, _worker_count, _worker_rr_cycler # <-- MODIFIED

    if _spdk_initialized: return
    _completion_thread = threading.Thread(target=_completion_loop_runner, name="SPDK-Completion-Thread", daemon=True)
    _completion_thread.start()
    while not _completion_loop or not _completion_loop.is_running():
        time.sleep(0.01)
    spdk_blob.init(bdev_name, json_config_path, reactor_mask, sock, _completion_loop)
    _spdk_initialized = True
    
    _worker_count = spdk_blob.get_worker_count()
    if _worker_count <= 0:
        unload()
        raise RuntimeError(f"SPDK initialized with zero I/O workers. Check reactor_mask: {reactor_mask}")
    _worker_rr_cycler = itertools.cycle(range(_worker_count))
    logging.info(f"SPDK initialized with {_worker_count} I/O workers. Round-robin scheduler is ready.")
    
    page_size = get_page_size()
    cluster_size = get_cluster_size()
    io_unit = get_io_unit_size()
    free_cluster_count = spdk_blob.get_free_cluster_count()
    _blob_size_in_clusters = math.ceil(_BLOB_SIZE_IN_BYTES / cluster_size)
    logging.info(f"Populating global blob pool with {_INITIAL_BLOB_COUNT} blobs...")
    try:
        _all_blob_ids = [_submit_meta_op_blocking(spdk_blob.create_async, _blob_size_in_clusters) for _ in range(_INITIAL_BLOB_COUNT)]
        _all_blob_handles = [_submit_meta_op_blocking(spdk_blob.open_async, bid) for bid in _all_blob_ids]
        for handle in _all_blob_handles:
            _free_blobs_queue.put(handle)
        logging.info(f"Global blob pool populated.")
        logging.info(f"IO unit size: {io_unit}, Page size: {page_size}, Cluster size: {cluster_size}, Total Size:{free_cluster_count * cluster_size / (1024**3):.2f} GB")
    except Exception as e:
        logging.error("Failed to populate global blob pool during init. Unloading...", exc_info=True)
        unload()
        raise e
    logging.info("SPDK global init successful.")

def unload() -> None:
    """Gracefully shuts down all resources and unloads SPDK."""
    global _spdk_initialized, _completion_loop, _completion_thread, _worker_count, _worker_rr_cycler # <-- MODIFIED
    if not _spdk_initialized: return
    logging.info("Cleaning up global blob pool...")
    try:
        for handle in _all_blob_handles:
             _submit_meta_op_blocking(spdk_blob.close_async, handle)
        logging.info(f"Closed all {len(_all_blob_handles)} blob handles.")
        for bid in _all_blob_ids:
            _submit_meta_op_blocking(spdk_blob.delete_async, bid)
        logging.info(f"Deleted all {len(_all_blob_ids)} blobs.")
    except Exception as e:
        logging.warning(f"An error occurred during blob cleanup, unload might fail: {e}", exc_info=True)
    _all_blob_handles.clear()
    _all_blob_ids.clear()
    spdk_blob.unload()
    _spdk_initialized = False

    _worker_count = 0      # <-- MODIFIED
    _worker_rr_cycler = None # <-- MODIFIED

    if _completion_loop and _completion_loop.is_running():
        _completion_loop.call_soon_threadsafe(_completion_loop.stop)
    if _completion_thread:
        _completion_thread.join()
    _completion_loop = None
    _completion_thread = None
    logging.info("SPDK unload complete.")


def get_blob(timeout: float = 5.0) -> int:
    """
    Gets a free blob handle from the pool. Blocks if the pool is empty.
    If the number of available blobs drops below a watermark, it triggers
    an asynchronous task to refill the pool.
    """
    try:
        handle = _free_blobs_queue.get(timeout=timeout)
        if _free_blobs_queue.qsize() < _LOW_WATER_MARK:
            _trigger_repopulation()
        return handle
    except Empty:
        raise RuntimeError(f"Failed to get a free blob handle within {timeout}s. Pool is empty and asynchronous repopulation might be too slow or has failed.")

def release_blob(handle: int):
    """Returns a blob handle to the free pool."""
    _free_blobs_queue.put(handle)

def write_async(handle: int, ptr: int, offset_bytes: int, size_bytes: int) -> Future:
    """Asynchronously write data to a blob."""
    if not _spdk_initialized: raise RuntimeError("SPDK is not initialized.")
    worker_id = _get_next_worker_id() 
    io_unit_size = get_io_unit_size()
    offset_units = offset_bytes // io_unit_size
    num_units = size_bytes // io_unit_size
    future = Future()
    spdk_blob.write_async(worker_id, handle, ptr, offset_units, num_units, future)
    return future

def read_async(handle: int, ptr: int, offset_bytes: int, size_bytes: int) -> Future:
    """Asynchronously read data from a blob."""
    if not _spdk_initialized: raise RuntimeError("SPDK is not initialized.")
    worker_id = _get_next_worker_id() 
    io_unit_size = get_io_unit_size()
    offset_units = offset_bytes // io_unit_size
    num_units = size_bytes // io_unit_size
    future = Future()
    spdk_blob.read_async(worker_id, handle, ptr, offset_units, num_units, future)
    return future

def write_batch_async(requests: List[Tuple[int, int, int, int]]) -> Future:
    """
    Asynchronously write a batch of data to blobs using a zero-copy ctypes buffer.
    """
    # print(f"write {len(requests)} kvchunks to spdk backend")
    if not _spdk_initialized: raise RuntimeError("SPDK is not initialized.")
    if not requests:
        f = Future()
        f.set_result(None)
        return f
    
    worker_id = _get_next_worker_id()
    io_unit_size = get_io_unit_size()
    batch_size = len(requests)
    c_requests_array = (_C_IO_Request * batch_size)()

    for i, (handle, ptr, offset, size) in enumerate(requests):
        req = c_requests_array[i]
        req.handle = handle
        req.payload = ptr
        req.offset_units = offset // io_unit_size
        req.num_units = size // io_unit_size
    
    array_ptr = ctypes.addressof(c_requests_array)
    
    future = Future()
    spdk_blob.write_batch_async(worker_id, array_ptr, batch_size, future)
    return future

def read_batch_async(requests: List[Tuple[int, int, int, int]]) -> Future:
    """
    Asynchronously read a batch of data from blobs using a zero-copy ctypes buffer.
    """
    # print(f"read {len(requests)} kvchunks from spdk backend")
    if not _spdk_initialized: raise RuntimeError("SPDK is not initialized.")
    if not requests:
        f = Future()
        f.set_result(None)
        return f

    worker_id = _get_next_worker_id()
    io_unit_size = get_io_unit_size()
    batch_size = len(requests)
    c_requests_array = (_C_IO_Request * batch_size)()

    for i, (handle, ptr, offset, size) in enumerate(requests):
        req = c_requests_array[i]
        req.handle = handle
        req.payload = ptr
        req.offset_units = offset // io_unit_size
        req.num_units = size // io_unit_size

    array_ptr = ctypes.addressof(c_requests_array)
    
    future = Future()
    spdk_blob.read_batch_async(worker_id, array_ptr, batch_size, future)
    return future

def alloc_io_buffer_view(size: int) -> Tuple[memoryview, int]:
    """Allocate a DMA-safe IO buffer and return it as a memoryview and a pointer."""
    if not _spdk_initialized: raise RuntimeError("SPDK is not initialized.")
    return spdk_blob.alloc_io_buffer_view(size)

def free_io_buffer(ptr: int) -> None: 
    """Free a DMA-safe IO buffer."""
    if not _spdk_initialized: return
    spdk_blob.free_io_buffer(ptr)

# --- Cached Metadata APIs ---
def get_page_size() -> int: 
    """Get the page size of the underlying blobstore (cached)."""
    global _page_size_cache
    if not _spdk_initialized: return 0
    if _page_size_cache == -1:
        with _metadata_cache_lock:
            if _page_size_cache == -1:
                _page_size_cache = spdk_blob.get_page_size()
    return _page_size_cache

def get_cluster_size() -> int:
    """Get the cluster size of the underlying blobstore (cached)."""
    global _cluster_size_cache
    if not _spdk_initialized: return 0
    if _cluster_size_cache == -1:
        with _metadata_cache_lock:
            if _cluster_size_cache == -1:
                _cluster_size_cache = spdk_blob.get_cluster_size()
    return _cluster_size_cache

def get_io_unit_size() -> int:
    """Get the I/O unit size of the underlying blobstore (cached)."""
    global _io_unit_size_cache
    if not _spdk_initialized: return 0
    if _io_unit_size_cache == -1:
        with _metadata_cache_lock:
            if _io_unit_size_cache == -1:
                _io_unit_size_cache = spdk_blob.get_io_unit_size()
    return _io_unit_size_cache