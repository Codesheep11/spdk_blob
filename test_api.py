import os
import sys
import logging
import time
import spdk_controller as spdk
import threading
from concurrent.futures import wait, Future
from typing import List, Tuple, Dict, Any
from threading import Lock

# --- 测试参数 ---
# 注意：为了更容易达到带宽上限，可以适当增加 NUM_TEST_THREADS
BDEV_NAME = "Nvme0n1"
JSON_CONFIG_FILE = "/home/yangxc/project/KVCache/spdk_blob/json/Nvme0n1.json"
# 确保你的 reactor mask 有足够的核给 SPDK worker
# 例如，如果 NUM_TEST_THREADS=3，你需要至少 1(main) + 3(workers) = 4 个核
REACTOR_MASK = "0xff000000000000" # 假设这个 mask 至少包含 4 个核
SOCK_PATH = "/var/tmp/spdk_test_api.sock"
IO_SIZE_KB = 56 * 1024
TOTAL_OPS_PER_THREAD = 1024
NUM_TEST_THREADS = 3
# IO_BATCH_SIZE 现在定义了每次批量提交的请求数量
IO_BATCH_SIZE = 128

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s [%(levelname)s] - %(message)s')


# ==============================================================================
# 核心修改：将 I/O 负载函数重写为批量提交模式
# ==============================================================================
def run_io_workload_batched(
    barriers: Dict[str, threading.Barrier],
    stop_event: threading.Event,
    progress_counter: Dict[str, int],
    progress_lock: Lock
) -> None:
    buffer_ptr = 0
    memview = None
    blob_handles = []

    try:
        # 确保线程已向 SPDK 注册，并获取 worker_id
        spdk.get_page_size()
        buffer_size = IO_SIZE_KB * 1024
        memview, buffer_ptr = spdk.alloc_io_buffer_view(buffer_size)

        # 改动点 1: 不再是只预分配一批，而是为当前线程预分配所有需要的 blob 句柄
        # 这简化了测试逻辑，避免了在循环中获取和释放 blob 的开销
        logging.debug(f"{threading.current_thread().name} pre-allocating {TOTAL_OPS_PER_THREAD} blob handles...")
        blob_handles = [spdk.get_blob(timeout=10) for _ in range(TOTAL_OPS_PER_THREAD)]
        logging.debug(f"{threading.current_thread().name} blob handles allocated.")

        # --- 写测试阶段 ---
        barriers['write_start'].wait()
        logging.debug(f"{threading.current_thread().name} starting BATCHED write loop...")

        # 改动点 2: 循环以 IO_BATCH_SIZE 为步长，构建并提交批次
        for i in range(0, TOTAL_OPS_PER_THREAD, IO_BATCH_SIZE):
            if stop_event.is_set():
                break

            # 1. 构建一个批次的请求列表
            batch_requests = []
            # 计算当前批次的结束索引
            end_index = min(i + IO_BATCH_SIZE, TOTAL_OPS_PER_THREAD)
            for j in range(i, end_index):
                handle = blob_handles[j]
                # 每个请求是 (handle, ptr, offset_bytes, size_bytes) 的元组
                batch_requests.append((handle, buffer_ptr, 0, buffer_size))

            # 2. 一次性提交整个批次
            if batch_requests:
                # 调用批量写入 API
                future = spdk.write_batch_async(batch_requests)
                # 阻塞等待这整个批次完成。对于纯吞吐量测试，这是最高效的方式。
                future.result(timeout=30)

                # 3. 更新进度
                with progress_lock:
                    progress_counter['write'] += len(batch_requests)
        
        # --- 读测试阶段 ---
        barriers['write_done'].wait()
        barriers['read_start'].wait()
        logging.debug(f"{threading.current_thread().name} starting BATCHED read loop...")

        for i in range(0, TOTAL_OPS_PER_THREAD, IO_BATCH_SIZE):
            if stop_event.is_set():
                break
            
            batch_requests = []
            end_index = min(i + IO_BATCH_SIZE, TOTAL_OPS_PER_THREAD)
            for j in range(i, end_index):
                handle = blob_handles[j]
                batch_requests.append((handle, buffer_ptr, 0, buffer_size))

            if batch_requests:
                # 调用批量读取 API
                future = spdk.read_batch_async(batch_requests)
                future.result(timeout=30)

                with progress_lock:
                    progress_counter['read'] += len(batch_requests)

    except Exception as e:
        logging.error(f"An error occurred during BATCHED workload ❌: {e}", exc_info=True)
        stop_event.set()
    finally:
        # 在线程结束时，一次性释放所有分配的 blob
        if blob_handles:
            for handle in blob_handles:
                spdk.release_blob(handle)
        if buffer_ptr:
            spdk.free_io_buffer(buffer_ptr)
        # 清理当前线程与 SPDK 的绑定
        spdk.cleanup_current_thread()


def run_test_phase():
    logging.info("=" * 70)
    logging.info(f"STARTING I/O BANDWIDTH TEST (BATCHED MODE)")
    logging.info("=" * 70)
    num_parties = NUM_TEST_THREADS + 1
    barriers = {
        'write_start': threading.Barrier(num_parties),
        'write_done': threading.Barrier(num_parties),
        'read_start': threading.Barrier(num_parties)
    }
    stop_event = threading.Event()
    threads = []

    progress_counter = {'write': 0, 'read': 0}
    progress_lock = Lock()

    for i in range(NUM_TEST_THREADS):
        thread = threading.Thread(
            # 改动点 3: 确保线程启动的是新的批量模式函数
            target=run_io_workload_batched,
            name=f"IO-Thread-{i}",
            args=(barriers, stop_event, progress_counter, progress_lock)
        )
        threads.append(thread)
        thread.start()

    total_ops_one_way = TOTAL_OPS_PER_THREAD * NUM_TEST_THREADS
    total_data_one_way_mb = (total_ops_one_way * IO_SIZE_KB) / 1024

    # ==================== 写性能测试阶段 ====================
    logging.info("-" * 60)
    logging.info(f"--- Starting Write Performance Test Phase ---")

    write_wall_clock_start = time.perf_counter()
    barriers['write_start'].wait()

    # 主线程的进度监控逻辑保持不变
    last_logged_write_ops = 0
    while progress_counter['write'] < total_ops_one_way:
        if progress_counter['write'] - last_logged_write_ops >= 256:
            logging.info(f"Progress: {progress_counter['write']}/{total_ops_one_way} writes completed.")
            last_logged_write_ops = progress_counter['write']
        if stop_event.is_set():
            break
        time.sleep(0.1)

    barriers['write_done'].wait()
    write_wall_clock_end = time.perf_counter()

    app_write_duration = write_wall_clock_end - write_wall_clock_start

    if app_write_duration > 0 and not stop_event.is_set():
        app_write_bw_mbs = total_data_one_way_mb / app_write_duration
        logging.info(f"Write Phase Wall-Clock Duration: {app_write_duration:.4f}s")
        logging.info(f"Write Bandwidth: {app_write_bw_mbs:.2f} MB/s")

    # ==================== 读性能测试阶段 ====================
    logging.info("-" * 60)
    logging.info(f"--- Starting Read Performance Test Phase ---")

    read_wall_clock_start = time.perf_counter()
    barriers['read_start'].wait()

    last_logged_read_ops = 0
    while progress_counter['read'] < total_ops_one_way:
        if progress_counter['read'] - last_logged_read_ops >= 256:
            logging.info(f"Progress: {progress_counter['read']}/{total_ops_one_way} reads completed.")
            last_logged_read_ops = progress_counter['read']
        if stop_event.is_set():
            break
        time.sleep(0.1)

    # 等待所有工作线程结束
    for thread in threads:
        thread.join()
    read_wall_clock_end = time.perf_counter()

    app_read_duration = read_wall_clock_end - read_wall_clock_start

    if app_read_duration > 0 and not stop_event.is_set():
        app_read_bw_mbs = total_data_one_way_mb / app_read_duration
        logging.info(f"Read Phase Wall-Clock Duration: {app_read_duration:.4f}s")
        logging.info(f"Read Bandwidth: {app_read_bw_mbs:.2f} MB/s")

    # ==================== 最终总结 ====================
    if stop_event.is_set():
        logging.error(f"Test phase FAILED due to an error in a worker thread.")
    else:
        logging.info("-" * 60)
        logging.info(f"--- Test phase COMPLETED SUCCESSFULLY ---")
    logging.info("\n")
    return not stop_event.is_set()

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