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
# 注意：为了更容易达到带宽上限，可以适当增加 NUM_TEST_THREADS
BDEV_NAME = "Raid0"
JSON_CONFIG_FILE = "/home/yangxc/project/KVCache/spdk_blob/json/Raid0.json"
REACTOR_MASK = "0xf0000000000000"
# REACTOR_MASK = "0xf"
SOCK_PATH = "/var/tmp/spdk_test_api.sock"
IO_SIZE_KB = 56 * 1024
TOTAL_OPS_PER_THREAD = 1024
NUM_TEST_THREADS = 2
# IO_BATCH_SIZE 定义了每次批量提交的请求数量
IO_BATCH_SIZE = 128

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s [%(levelname)s] - %(message)s')


def run_io_workload_batched(
    start_barrier: threading.Barrier, # 简化为单个屏障，用于同步读取开始
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
        # 确保线程已向 SPDK 注册
        spdk.get_page_size()
        buffer_size = IO_SIZE_KB * 1024
        
        # 分配 I/O 缓冲区
        memview, buffer_ptr = spdk.alloc_io_buffer_view(buffer_size)

        # 预分配所有需要的 blob 句柄。
        # 注意：此处假设这些 Blob 已经被预先写入了数据，否则读取测试将无效。
        logging.debug(f"{threading.current_thread().name} pre-allocating {TOTAL_OPS_PER_THREAD} blob handles for reading...")
        # 假设 get_blob() 能获取到有效的、已包含数据的 blob 句柄
        blob_handles = [spdk.get_blob(timeout=10) for _ in range(TOTAL_OPS_PER_THREAD)]
        logging.debug(f"{threading.current_thread().name} blob handles allocated.")

        # --- 读测试阶段 ---
        start_barrier.wait() # 等待所有线程准备就绪，同步开始读取
        logging.info(f"{threading.current_thread().name} starting BATCHED read loop...")

        # 循环以 IO_BATCH_SIZE 为步长，构建并提交读取批次
        for i in range(0, TOTAL_OPS_PER_THREAD, IO_BATCH_SIZE):
            if stop_event.is_set():
                break
            
            # 1. 构建一个批次的请求列表
            batch_requests = []
            end_index = min(i + IO_BATCH_SIZE, TOTAL_OPS_PER_THREAD)
            for j in range(i, end_index):
                handle = blob_handles[j]
                # 每个请求是 (handle, ptr, offset_bytes, size_bytes) 的元组
                # 注意：读取时 ptr 指向的缓冲区将被填充数据
                batch_requests.append((handle, buffer_ptr, 0, buffer_size))

            # 2. 一次性提交整个批次
            if batch_requests:
                # 调用批量读取 API
                future = spdk.read_batch_async(batch_requests)
                # 阻塞等待这整个批次完成
                future.result(timeout=30)

                # 3. 更新进度
                with progress_lock:
                    progress_counter['read'] += len(batch_requests)

    except Exception as e:
        logging.error(f"An error occurred during BATCHED workload ❌: {e}", exc_info=True)
        stop_event.set()
    finally:
        # 线程结束时清理资源
        if blob_handles:
            for handle in blob_handles:
                spdk.release_blob(handle)
        if buffer_ptr:
            spdk.free_io_buffer(buffer_ptr)
        spdk.cleanup_current_thread()


def run_test_phase():
    """
    主控制函数，设置测试环境并监控进度。
    """
    logging.info("=" * 70)
    logging.info(f"STARTING I/O READ BANDWIDTH TEST (BATCHED MODE)")
    logging.info("=" * 70)
    
    num_parties = NUM_TEST_THREADS + 1
    # 只需要一个屏障来同步所有线程的读取开始
    test_start_barrier = threading.Barrier(num_parties)
    
    stop_event = threading.Event()
    threads = []

    # 仅需要 'read' 计数器
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
    total_data_mb = (total_ops * IO_SIZE_KB) / 1024 # 计算总共读取的数据量 (MB)

    # ==================== 读性能测试阶段 ====================
    logging.info("-" * 60)
    logging.info(f"--- Starting Read Performance Test Phase ---")
    logging.info(f"Total I/O Operations to perform: {total_ops}")
    logging.info(f"Total Data to read: {total_data_mb:.2f} MB")

    read_wall_clock_start = time.perf_counter()
    test_start_barrier.wait() # 启动所有工作线程的读取循环

    # 主线程监控进度
    last_logged_read_ops = 0
    while progress_counter['read'] < total_ops:
        if progress_counter['read'] - last_logged_read_ops >= 256:
            logging.info(f"Progress: {progress_counter['read']}/{total_ops} reads completed.")
            last_logged_read_ops = progress_counter['read']
        if stop_event.is_set():
            break
        time.sleep(0.1)

    # 等待所有工作线程结束
    for thread in threads:
        thread.join()
    read_wall_clock_end = time.perf_counter()

    app_read_duration = read_wall_clock_end - read_wall_clock_start
    success = not stop_event.is_set()

    # ==================== 最终总结 ====================
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
    
    # 初始化 spdk_controller
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
