import os
import sys
import logging
import time
import spdk_controller as spdk
from typing import List, Tuple, Deque
from collections import deque

# --- 测试参数 ---
BDEV_NAME = "Raid0"
JSON_CONFIG_FILE = "/home/yangxc/project/KVCache/spdk_blob/json/Raid0.json"
REACTOR_MASK = "0xff0000000000000"
SOCK_PATH = "/var/tmp/spdk_micro_test.sock"
IO_SIZE_KB = 56 * 1024  # 假设每个 "kvchunk" 的大小
WORKLOAD_FILE = "/home/yangxc/project/KVCache/test/ins2.txt" # 您的负载指令文件
QUEUE_DEPTH = 32 # 维护32个在处理的请求
WRITE_BATCH_SIZE = 32 # 批量写的批次大小

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_workload(file_path: str) -> List[str]:
    """
    解析负载指令文件 (ins.txt)，将其转换为一个扁平化的操作列表。
    """
    if not os.path.exists(file_path):
        logging.error(f"Workload file not found: {file_path}")
        sys.exit(1)

    all_requests = []
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line: continue

            parts = line.split()
            if len(parts) < 2:
                logging.warning(f"Skipping malformed line {line_num}: {line}")
                continue

            operation = parts[0].lower()
            try:
                count = int(parts[1])
                if operation in ['read', 'write']:
                    all_requests.extend([operation] * count)
                else:
                    logging.warning(f"Skipping unknown operation on line {line_num}: {operation}")
            except ValueError:
                logging.warning(f"Skipping line {line_num} with invalid count: {parts[1]}")

    logging.info(f"Successfully parsed workload. Total requests: {len(all_requests)}.")
    return all_requests


def run_individual_requests_test(all_requests: List[str], buffer_ptr: int, buffer_size: int):
    """
    测试1：所有请求都单独提交，保持固定队列深度。
    """
    logging.info("=" * 70)
    logging.info(f"STARTING TEST 1: INDIVIDUAL REQUESTS (Queue Depth: {QUEUE_DEPTH})")
    logging.info("=" * 70)

    total_requests_to_process = len(all_requests)
    if total_requests_to_process == 0:
        return

    inflight_requests: Deque[Tuple[any, any]] = deque() # 存储 (future, blob_handle)
    submitted_count = 0
    completed_count = 0

    wall_clock_start = time.perf_counter()

    while completed_count < total_requests_to_process:
        while len(inflight_requests) < QUEUE_DEPTH and submitted_count < total_requests_to_process:
            operation = all_requests[submitted_count]
            handle = spdk.get_blob(timeout=10)
            
            if operation == 'read':
                future = spdk.read_async(handle, buffer_ptr, 0, buffer_size)
            else: # write
                future = spdk.write_async(handle, buffer_ptr, 0, buffer_size)
            
            inflight_requests.append((future, handle))
            submitted_count += 1

        if inflight_requests:
            future, handle = inflight_requests.popleft()
            try:
                future.result(timeout=30)
            finally:
                spdk.release_blob(handle)
                completed_count += 1
                
                if completed_count % 200 == 0 or completed_count == total_requests_to_process:
                     logging.info(f"[Individual] Progress: Completed {completed_count}/{total_requests_to_process} requests...")

    wall_clock_end = time.perf_counter()
    total_duration = wall_clock_end - wall_clock_start
    iops = total_requests_to_process / total_duration if total_duration > 0 else float('inf')
    
    logging.info("-" * 70)
    logging.info(f"--- INDIVIDUAL REQUESTS TEST COMPLETED ---")
    logging.info(f"Total requests executed: {total_requests_to_process}")
    logging.info(f"Total wall-clock duration: {total_duration:.4f} seconds")
    logging.info(f"IOPS: {iops:.2f}")
    logging.info("-" * 70)


def run_batched_write_test(all_requests: List[str], buffer_ptr: int, buffer_size: int):
    """
    测试2：写请求累计到指定数量后批量提交，读请求单独提交。
    """
    logging.info("\n" + "=" * 70)
    logging.info(f"STARTING TEST 2: BATCHED WRITES (Queue Depth: {QUEUE_DEPTH}, Batch Size: {WRITE_BATCH_SIZE})")
    logging.info("=" * 70)
    
    total_requests = len(all_requests)
    if total_requests == 0:
        return

    inflight_queue = deque() # 存储 {'future': ..., 'handles': [...], 'count': ...}
    write_batch_params = []
    write_batch_handles = []

    req_iterator = iter(all_requests)
    current_req = next(req_iterator, None)
    
    completed_count = 0
    start_time = time.perf_counter()

    while completed_count < total_requests:
        # 持续提交新任务，直到队列满或所有请求都已处理
        while len(inflight_queue) < QUEUE_DEPTH and current_req is not None:
            if current_req == 'read':
                # 读请求会中断并刷新当前累积的写请求批次，以保证顺序
                if write_batch_params:
                    future = spdk.write_batch_async(write_batch_params)
                    inflight_queue.append({'future': future, 'handles': write_batch_handles, 'count': len(write_batch_params)})
                    write_batch_params, write_batch_handles = [], []
                    if len(inflight_queue) >= QUEUE_DEPTH:
                        break
                
                handle = spdk.get_blob(timeout=10)
                future = spdk.read_async(handle, buffer_ptr, 0, buffer_size)
                inflight_queue.append({'future': future, 'handles': [handle], 'count': 1})
                current_req = next(req_iterator, None)

            else: # 是 'write' 请求
                handle = spdk.get_blob(timeout=10)
                write_batch_params.append((handle, buffer_ptr, 0, buffer_size))
                write_batch_handles.append(handle)
                current_req = next(req_iterator, None)

                if len(write_batch_params) == WRITE_BATCH_SIZE:
                    future = spdk.write_batch_async(write_batch_params)
                    inflight_queue.append({'future': future, 'handles': write_batch_handles, 'count': WRITE_BATCH_SIZE})
                    write_batch_params, write_batch_handles = [], []
        
        # 如果所有请求都已迭代完，但仍有未提交的写批次，则提交它
        if current_req is None and write_batch_params:
            future = spdk.write_batch_async(write_batch_params)
            inflight_queue.append({'future': future, 'handles': write_batch_handles, 'count': len(write_batch_params)})
            write_batch_params, write_batch_handles = [], []

        # 从队列头部取出一个已完成的任务进行处理
        if inflight_queue:
            completed_unit = inflight_queue.popleft()
            try:
                completed_unit['future'].result(timeout=60) # 批量操作超时时间适当延长
            finally:
                for handle in completed_unit['handles']:
                    spdk.release_blob(handle)
                completed_count += completed_unit['count']
                
                if completed_count % 200 == 0 or completed_count >= total_requests:
                    logging.info(f"[Batched] Progress: Completed {completed_count}/{total_requests} requests...")
    
    end_time = time.perf_counter()
    duration = end_time - start_time
    iops = total_requests / duration if duration > 0 else float('inf')
    
    logging.info("-" * 70)
    logging.info(f"--- BATCHED WRITE TEST COMPLETED ---")
    logging.info(f"Total requests executed: {total_requests}")
    logging.info(f"Total wall-clock duration: {duration:.4f} seconds")
    logging.info(f"IOPS: {iops:.2f}")
    logging.info("-" * 70)


def main():
    if not os.path.exists(JSON_CONFIG_FILE):
        sys.exit(f"ERROR: SPDK config file not found: {JSON_CONFIG_FILE}")

    all_requests = parse_workload(WORKLOAD_FILE)
    if not all_requests:
        sys.exit("ERROR: No valid workload instructions found in the file.")

    spdk.init(BDEV_NAME, JSON_CONFIG_FILE, REACTOR_MASK, SOCK_PATH)
    
    buffer_ptr = 0
    memview = None
    
    try:
        buffer_size = IO_SIZE_KB * 1024
        memview, buffer_ptr = spdk.alloc_io_buffer_view(buffer_size)
        memview[:] = b'\xAB' * buffer_size

        # --- 运行对比测试 ---
        run_individual_requests_test(all_requests, buffer_ptr, buffer_size)
        run_batched_write_test(all_requests, buffer_ptr, buffer_size)

    except Exception as e:
        logging.error(f"An error occurred in the main orchestrator ❌: {e}", exc_info=True)
    finally:
        logging.info("--- Cleaning up resources ---")
        if buffer_ptr:
            spdk.free_io_buffer(buffer_ptr)
        spdk.unload()


if __name__ == "__main__":
    main()

