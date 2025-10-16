import os
import sys
import logging
import time
import spdk_controller as spdk
import threading
from concurrent.futures import Future
from typing import List, Tuple, Dict, Any
from threading import Lock

# --- 测试参数 ---
BDEV_NAME = "Raid0"
JSON_CONFIG_FILE = "/home/yangxc/project/KVCache/spdk_blob/json/Raid0.json"
REACTOR_MASK = "0xff0000000000000"
SOCK_PATH = "/var/tmp/spdk_test_api.sock"
IO_SIZE_KB = 56 * 1024
TOTAL_OPS_PER_THREAD = 1024
NUM_TEST_THREADS = 4
IO_BATCH_SIZE = 128

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s [%(levelname)s] - %(message)s')


def run_io_workload_batched(
    start_barrier: threading.Barrier,
    stop_event: threading.Event,
    progress_counter: Dict[str, int],
    progress_lock: Lock
) -> None:
    """
    工作线程函数：仅执行批量读取 (Read) I/O 操作。
    """
    buffer_ptr = 0
    memview = None
    blob_handles = []

    try:
        # --- MODIFIED ---
        # 线程不再需要向SPDK注册，因此下面的调用不再是必须的
        # spdk.get_page_size() 
        buffer_size = IO_SIZE_KB * 1024
        
        memview, buffer_ptr = spdk.alloc_io_buffer_view(buffer_size)

        logging.debug(f"{threading.current_thread().name} pre-allocating {TOTAL_OPS_PER_THREAD} blob handles for reading...")
        blob_handles = [spdk.get_blob(timeout=10) for _ in range(TOTAL_OPS_PER_THREAD)]
        logging.debug(f"{threading.current_thread().name} blob handles allocated.")

        start_barrier.wait()
        logging.info(f"{threading.current_thread().name} starting BATCHED read loop...")

        for i in range(0, TOTAL_OPS_PER_THREAD, IO_BATCH_SIZE):
            if stop_event.is_set():
                break
            
            batch_requests = []
            end_index = min(i + IO_BATCH_SIZE, TOTAL_OPS_PER_THREAD)
            for j in range(i, end_index):
                handle = blob_handles[j]
                batch_requests.append((handle, buffer_ptr, 0, buffer_size))

            if batch_requests:
                future = spdk.read_batch_async(batch_requests)
                future.result(timeout=30)

                with progress_lock:
                    progress_counter['read'] += len(batch_requests)

    except Exception as e:
        logging.error(f"An error occurred during BATCHED workload ❌: {e}", exc_info=True)
        stop_event.set()
    finally:
        if blob_handles:
            for handle in blob_handles:
                spdk.release_blob(handle)
        if buffer_ptr:
            spdk.free_io_buffer(buffer_ptr)
        # --- DELETED ---: cleanup_current_thread 函数已被移除
        # spdk.cleanup_current_thread()


def run_test_phase():
    """
    主控制函数，设置测试环境并监控进度。
    """
    logging.info("=" * 70)
    logging.info(f"STARTING I/O READ BANDWIDTH TEST (BATCHED MODE)")
    logging.info("=" * 70)
    
    num_parties = NUM_TEST_THREADS + 1
    test_start_barrier = threading.Barrier(num_parties)
    
    stop_event = threading.Event()
    threads = []

    progress_counter = {'read': 0}
    progress_lock = Lock()

    for i in range(NUM_TEST_THREADS):
        thread = threading.Thread(
            target=run_io_workload_batched,
            name=f"IO-Thread-{i}",
            args=(test_start_barrier, stop_event, progress_counter, progress_lock)
        )
        threads.append(thread)
        thread.start()

    total_ops = TOTAL_OPS_PER_THREAD * NUM_TEST_THREADS
    total_data_mb = (total_ops * IO_SIZE_KB) / 1024

    logging.info("-" * 60)
    logging.info(f"--- Starting Read Performance Test Phase ---")
    logging.info(f"Total I/O Operations to perform: {total_ops}")
    logging.info(f"Total Data to read: {total_data_mb:.2f} MB")

    read_wall_clock_start = time.perf_counter()
    test_start_barrier.wait()

    last_logged_read_ops = 0
    while progress_counter['read'] < total_ops:
        if progress_counter['read'] - last_logged_read_ops >= 256:
            logging.info(f"Progress: {progress_counter['read']}/{total_ops} reads completed.")
            last_logged_read_ops = progress_counter['read']
        if stop_event.is_set():
            break
        time.sleep(0.1)

    for thread in threads:
        thread.join()
    read_wall_clock_end = time.perf_counter()

    app_read_duration = read_wall_clock_end - read_wall_clock_start
    success = not stop_event.is_set()

    logging.info("-" * 60)
    if success and app_read_duration > 0:
        app_read_bw_mbs = total_data_mb / app_read_duration
        logging.info(f"Read Phase Wall-Clock Duration: {app_read_duration:.4f}s")
        logging.info(f"Read Bandwidth: {app_read_bw_mbs:.2f} MB/s")
        logging.info(f"--- Test phase COMPLETED SUCCESSFULLY ---")
    else:
        logging.error(f"Test phase FAILED or duration was zero.")
        
    logging.info("\n")
    return success

def main():
    if not os.path.exists(JSON_CONFIG_FILE):
        sys.exit(f"ERROR: SPDK config file not found: {JSON_CONFIG_FILE}")
    
    spdk.init(BDEV_NAME, JSON_CONFIG_FILE, REACTOR_MASK, SOCK_PATH)
    
    try:
        success = run_test_phase()
        if not success:
            logging.error(f"Test phase failed.")
    finally:
        logging.info("--- Cleaning up global SPDK resources ---")
        spdk.unload()


if __name__ == "__main__":
    main()