import os
import sys
import logging
import time
import spdk_controller as spdk
import threading
from concurrent.futures import wait, Future
from typing import List, Tuple, Dict, Any
import random
import subprocess
import json
from tqdm import tqdm 
from threading import Lock 

# --- 测试配置 ---
BDEV_NAME = "Raid0"
JSON_CONFIG_FILE = "/home/yangxc/project/KVCache/spdk_blob/json/Raid0.json"

# BDEV_NAME = "Nvme0n1"
# JSON_CONFIG_FILE = "/home/yangxc/project/KVCache/spdk_blob/json/Nvme0n1.json"
REACTOR_MASK = "0xf000000000000"
SOCK_PATH = "/var/tmp/spdk_test_api.sock"
IO_SIZE_KB = 56 * 1024
TOTAL_OPS_PER_THREAD = 1024
NUM_TEST_THREADS = 2
IO_BATCH_SIZE = 64 # 对于单个IO模式，这代表一次提交多少个独立的IO；对于批处理模式，这代表一个批次包含多少IO
RPC_SCRIPT_PATH = "/home/yangxc/project/KVCache/spdk/scripts/rpc.py"

# <--- 优化点: 定义测试模式，可以轻松切换或运行所有模式
TEST_MODES = ["single", "batch"] # "single" for one-by-one async IO, "batch" for batch async IO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s [%(levelname)s] - %(message)s')


# --- RPC 调用辅助函数 (保持不变) ---
def get_bdev_iostat(sock_path: str, bdev_name: str) -> Dict[str, Any] | None:
    """
    通过调用 rpc.py 脚本获取指定 bdev 的 I/O 统计信息。
    现在返回更完整的数据，包括用于精确计时的 ticks。
    """
    if not os.path.exists(RPC_SCRIPT_PATH):
        logging.error(f"SPDK RPC script not found at: {RPC_SCRIPT_PATH}")
        return None

    command = [
        sys.executable,
        RPC_SCRIPT_PATH,
        "-s", sock_path,
        "bdev_get_iostat",
        "-b", bdev_name,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=10)
        data = json.loads(result.stdout)
        
        tick_rate = data.get("tick_rate", 0)
        if tick_rate == 0:
            logging.error("Could not get tick_rate from RPC output.")
            return None

        for bdev_stats in data.get("bdevs", []):
            if bdev_stats.get("name") == bdev_name:
                return {
                    "bytes_read": bdev_stats.get("bytes_read", 0),
                    "bytes_written": bdev_stats.get("bytes_written", 0),
                    "ticks": data.get("ticks", 0),
                    "tick_rate": tick_rate,
                }
        
        logging.error(f"Bdev '{bdev_name}' not found in RPC iostat output.")
        return None
    except Exception as e:
        logging.error(f"RPC command execution failed: {e}")
        return None


def run_io_workload(
    mode: str,
    barriers: Dict[str, threading.Barrier], 
    stop_event: threading.Event, 
    result_list: list,
    progress_counter: Dict[str, int],
    progress_lock: Lock
) -> None:
    """每个 I/O 线程执行的工作负载函数，由多个barrier控制阶段"""
    thread_name = threading.current_thread().name
    buffer_ptr = 0
    memview = None
    
    try:
        # 1. 注册线程并分配资源
        spdk.get_page_size() # 确保线程已注册
        buffer_size = IO_SIZE_KB * 1024
        memview, buffer_ptr = spdk.alloc_io_buffer_view(buffer_size)
        base_test_data = f"PAYLOAD_FROM_{thread_name}".encode('utf-8')

        # --- 写测试阶段 ---
        barriers['write_start'].wait()
        
        # 提前获取所有需要的blob
        test_sequence: List[Tuple[int, int]] = []
        # 根据总操作数和每个blob能执行的操作数（这里简化为1个批次）来决定需要多少blob
        num_blobs_needed = (TOTAL_OPS_PER_THREAD + IO_BATCH_SIZE - 1) // IO_BATCH_SIZE
        for i in range(num_blobs_needed):
            handle = spdk.get_blob(timeout=10)
            test_sequence.append((handle, i))

        write_start_time = time.perf_counter()
        ops_written = 0
        for handle, unique_id in test_sequence:
            if stop_event.is_set() or ops_written >= TOTAL_OPS_PER_THREAD: break
            
            memview[:len(base_test_data)] = base_test_data
            memview[:8] = unique_id.to_bytes(8, 'little')

            ops_in_this_batch = min(IO_BATCH_SIZE, TOTAL_OPS_PER_THREAD - ops_written)
            
            if mode == "batch":
                write_requests = [(handle, buffer_ptr, 0, buffer_size) for _ in range(ops_in_this_batch)]
                write_future = spdk.write_batch_async(write_requests)
                write_future.result(timeout=15)
            
            elif mode == "single":
                # <--- 优化点: 单个IO API测试逻辑 ---
                futures: List[Future] = []
                for _ in range(ops_in_this_batch):
                    # 提交单个写请求，不阻塞
                    future = spdk.write_async(handle, buffer_ptr, 0, buffer_size)
                    futures.append(future)
                # 等待这一"批"独立的IO全部完成
                wait(futures, timeout=15)
                # 检查每个future的结果，确保没有异常抛出
                for f in futures:
                    f.result()
            
            ops_written += ops_in_this_batch
            # <--- 优化点: 更新进度 ---
            with progress_lock:
                progress_counter['write'] += ops_in_this_batch

        total_write_time = time.perf_counter() - write_start_time
        
        barriers['write_done'].wait()

        # --- 读测试阶段 ---
        barriers['read_start'].wait()
        
        random.shuffle(test_sequence)
        
        read_start_time = time.perf_counter()
        ops_read = 0
        for handle, unique_id in test_sequence:
            if stop_event.is_set() or ops_read >= TOTAL_OPS_PER_THREAD: break

            memview[:] = b'\x00' * buffer_size
            ops_in_this_batch = min(IO_BATCH_SIZE, TOTAL_OPS_PER_THREAD - ops_read)
            
            if mode == "batch":
                read_requests = [(handle, buffer_ptr, 0, buffer_size) for _ in range(ops_in_this_batch)]
                read_future = spdk.read_batch_async(read_requests)
                read_future.result(timeout=15)
            
            elif mode == "single":
                # <--- 优化点: 单个IO API测试逻辑 ---
                futures: List[Future] = []
                for _ in range(ops_in_this_batch):
                    future = spdk.read_async(handle, buffer_ptr, 0, buffer_size)
                    futures.append(future)
                wait(futures, timeout=15)
                for f in futures:
                    f.result()

            expected_data_head = unique_id.to_bytes(8, 'little')
            if expected_data_head != bytes(memview[:8]):
                logging.error(f"Data validation FAILED on handle {handle}!")
                stop_event.set()
                break
            
            ops_read += ops_in_this_batch
            # <--- 优化点: 更新进度 ---
            with progress_lock:
                progress_counter['read'] += ops_in_this_batch

        total_read_time = time.perf_counter() - read_start_time

    except Exception as e:
        logging.error(f"An error occurred during workload ❌: {e}", exc_info=True)
        stop_event.set()
    finally:
        if 'test_sequence' in locals():
            for handle, _ in test_sequence:
                spdk.release_blob(handle)
        if buffer_ptr:
            spdk.free_io_buffer(buffer_ptr)
        spdk.cleanup_current_thread()
        if 'total_write_time' in locals() and 'total_read_time' in locals():
            result_list.append((thread_name, total_write_time, total_read_time))


# <--- 优化点: 将核心测试流程封装成一个函数 ---
def run_test_phase(mode: str):
    """为指定的模式 (single/batch) 运行完整的测试流程"""
    logging.info("=" * 70)
    logging.info(f"  STARTING TEST PHASE: [{mode.upper()}] I/O API  ")
    logging.info("=" * 70)

    # --- 设置同步工具 ---
    num_parties = NUM_TEST_THREADS + 1
    barriers = {
        'write_start': threading.Barrier(num_parties),
        'write_done': threading.Barrier(num_parties),
        'read_start': threading.Barrier(num_parties)
    }
    stop_event = threading.Event()
    threads = []
    results: List[Tuple[str, float, float]] = []

    # --- 进度条所需的共享资源 ---
    progress_counter = {'write': 0, 'read': 0}
    progress_lock = Lock()

    # --- 创建并启动I/O工作线程 ---
    for i in range(NUM_TEST_THREADS):
        thread = threading.Thread(
            target=run_io_workload,
            name=f"IO-Thread-{i}",
            args=(mode, barriers, stop_event, results, progress_counter, progress_lock)
        )
        threads.append(thread)
        thread.start()

    total_ops_one_way = TOTAL_OPS_PER_THREAD * NUM_TEST_THREADS
    total_data_one_way_mb = (total_ops_one_way * IO_SIZE_KB) / 1024

    # ==================== 写性能测试阶段 ====================
    logging.info("-" * 60)
    logging.info(f"--- [{mode.upper()}] Starting Write Performance Test Phase ---")
    stats_before_write = get_bdev_iostat(SOCK_PATH, BDEV_NAME)
    
    write_wall_clock_start = time.perf_counter()
    barriers['write_start'].wait()
    
    # <--- 优化点: 启动进度条 ---
    with tqdm(total=total_ops_one_way, desc=f"Writing ({mode})") as pbar:
        while progress_counter['write'] < total_ops_one_way:
            pbar.n = progress_counter['write']
            pbar.refresh()
            if stop_event.is_set(): break
            time.sleep(0.1)
        pbar.n = progress_counter['write'] # 确保进度条达到100%
        pbar.refresh()

    barriers['write_done'].wait()
    write_wall_clock_end = time.perf_counter()
    stats_after_write = get_bdev_iostat(SOCK_PATH, BDEV_NAME)
    
    app_write_duration = write_wall_clock_end - write_wall_clock_start

    if stats_before_write and stats_after_write and app_write_duration > 0:
        ticks_diff = stats_after_write['ticks'] - stats_before_write['ticks']
        tick_rate = stats_after_write['tick_rate']
        rpc_write_duration = ticks_diff / tick_rate if tick_rate > 0 else 0
        bytes_written = stats_after_write['bytes_written'] - stats_before_write['bytes_written']
        app_write_bw_mbs = total_data_one_way_mb / app_write_duration
        rpc_write_bw_mbs = (bytes_written / (1024 * 1024)) / rpc_write_duration if rpc_write_duration > 0 else 0
        
        logging.info(f"Write Phase Wall-Clock Duration: {app_write_duration:.4f}s")
        logging.info(f" ↳ Application-Level Write Bandwidth: {app_write_bw_mbs:.2f} MB/s")
        logging.info(f"Write Phase Device-Level Duration (from ticks): {rpc_write_duration:.4f}s")
        logging.info(f" ↳ Device-Level Write Bandwidth (RPC): {rpc_write_bw_mbs:.2f} MB/s")
    
    # ==================== 读性能测试阶段 ====================
    logging.info("-" * 60)
    logging.info(f"--- [{mode.upper()}] Starting Read Performance Test Phase ---")
    
    read_wall_clock_start = time.perf_counter()
    barriers['read_start'].wait()
    
    # <--- 优化点: 启动读进度条 ---
    with tqdm(total=total_ops_one_way, desc=f"Reading ({mode}) ") as pbar:
        while progress_counter['read'] < total_ops_one_way:
            pbar.n = progress_counter['read']
            pbar.refresh()
            if stop_event.is_set(): break
            time.sleep(0.1)
        pbar.n = progress_counter['read'] # 确保进度条达到100%
        pbar.refresh()

    for thread in threads:
        thread.join()
    read_wall_clock_end = time.perf_counter()
    stats_after_read = get_bdev_iostat(SOCK_PATH, BDEV_NAME)
    
    app_read_duration = read_wall_clock_end - read_wall_clock_start
    
    if stats_after_write and stats_after_read and app_read_duration > 0:
        ticks_diff = stats_after_read['ticks'] - stats_after_write['ticks']
        tick_rate = stats_after_read['tick_rate']
        rpc_read_duration = ticks_diff / tick_rate if tick_rate > 0 else 0
        bytes_read = stats_after_read['bytes_read'] - stats_after_write['bytes_read']
        app_read_bw_mbs = total_data_one_way_mb / app_read_duration
        rpc_read_bw_mbs = (bytes_read / (1024 * 1024)) / rpc_read_duration if rpc_read_duration > 0 else 0

        logging.info(f"Read Phase Wall-Clock Duration: {app_read_duration:.4f}s")
        logging.info(f" ↳ Application-Level Read Bandwidth: {app_read_bw_mbs:.2f} MB/s")
        logging.info(f"Read Phase Device-Level Duration (from ticks): {rpc_read_duration:.4f}s")
        logging.info(f" ↳ Device-Level Read Bandwidth (RPC): {rpc_read_bw_mbs:.2f} MB/s")
        
    # ==================== 最终总结 ====================
    if stop_event.is_set():
        logging.error(f"Test phase [{mode.upper()}] FAILED due to an error in a worker thread.")
    else:
        logging.info("-" * 60)
        logging.info(f"--- [{mode.upper()}] Per-Thread Internal Timers (for reference) ---")
        for thread_name, write_time, read_time in results:
            logging.info(f"  [{thread_name}] Actual Write Time: {write_time:.2f}s, Actual Read Time: {read_time:.2f}s")
        logging.info(f"--- Test phase [{mode.upper()}] COMPLETED SUCCESSFULLY ---")
    logging.info("\n")
    return not stop_event.is_set()

def main():
    if not os.path.exists(JSON_CONFIG_FILE):
        sys.exit(f"ERROR: SPDK config file not found: {JSON_CONFIG_FILE}")

    # 注意: 您的测试文件中这里模块名写错了，我已修正。
    # 应该是 spdk.init(...) 而不是 spdk_controller.init(...)
    # 假设您的 __init__.py 文件名为 spdk_controller.py
    spdk.init(BDEV_NAME, JSON_CONFIG_FILE, REACTOR_MASK, SOCK_PATH)
    
    try:
        for mode in TEST_MODES:
            success = run_test_phase(mode)
            if not success:
                logging.error(f"Stopping tests because phase [{mode.upper()}] failed.")
                break
    finally:
        logging.info("--- Cleaning up global SPDK resources ---")
        spdk.unload()


if __name__ == "__main__":
    if not os.path.exists(RPC_SCRIPT_PATH):
        sys.exit(f"ERROR: Please edit 'RPC_SCRIPT_PATH' in this script to point to your 'rpc.py'.")
    main()