import os
import sys
import logging
import time
import subprocess
import json

# 定义全局常量
BDEV_NAME = "Nvme0n1" 
# BDEV_NAME = "Raid0" 
RPC_SCRIPT_PATH = "/home/yangxc/project/KVCache/spdk/scripts/rpc.py" # SPDK rpc.py脚本的路径
SOCK_PATH = "/var/tmp/spdk_test_api.sock" # SPDK RPC Unix套接字路径
OUTPUT_FILE = "tmp.txt" # 输出文件，用于保存监控数据

# 定义轮询和RPC超时相关的常量
# 轮询间隔更改为0.1秒
POLL_INTERVAL_S = 0.1 # I/O数据获取的轮询间隔（秒）
INITIAL_RPC_TIMEOUT_S = 10 # 初始RPC调用的超时时间（秒）
POLL_RPC_TIMEOUT_S = 5 # 持续监控时，每次RPC调用的超时时间（秒）

# 配置日志记录器，设置日志级别和格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')


def get_bdev_iostat(sock_path: str, bdev_name: str, rpc_timeout: int, log_errors: bool = True) -> dict | None:
    """
    通过SPDK RPC获取指定Bdev的I/O统计信息。
    此版本正确处理'ticks'和'tick_rate'在JSON顶层的情况。

    Args:
        sock_path: SPDK RPC套接字的路径。
        bdev_name: 要查询的Bdev名称。
        rpc_timeout: RPC调用失败时的重试总超时时间（秒）。
        log_errors: 是否在失败时记录错误日志。

    Returns:
        如果成功，返回包含'bytes_read'、'bytes_written'、'ticks'和'tick_rate'的字典；
        否则返回None。
    """
    # 检查rpc.py脚本路径是否存在
    if not os.path.exists(RPC_SCRIPT_PATH):
        if log_errors:
            logging.error(f"SPDK RPC script not found at: {RPC_SCRIPT_PATH}")
        return None

    # 构建调用rpc.py脚本的命令行
    command = [
        sys.executable, # 使用当前Python解释器
        RPC_SCRIPT_PATH,
        "-s", sock_path, # 指定SPDK套接字
        "bdev_get_iostat", # RPC方法：获取iostat
        "-b", bdev_name, # 指定Bdev设备
    ]
    
    start_time = time.time()
    # 循环重试，直到超时
    while time.time() - start_time < rpc_timeout:
        try:
            # 运行子进程命令
            result = subprocess.run(command, capture_output=True, text=True, check=True, timeout=5)
            data = json.loads(result.stdout)

            # 从顶层获取 ticks 和 tick_rate
            ticks = data.get("ticks", 0)
            tick_rate = data.get("tick_rate", 0)

            # 遍历JSON数据中的所有Bdev统计信息
            for bdev_stats in data.get("bdevs", []):
                # 找到与目标Bdev名称匹配的条目
                if bdev_stats.get("name") == bdev_name:
                    # 返回包含所有必要数据的字典
                    return {
                        "bytes_read": bdev_stats.get("bytes_read", 0),
                        "bytes_written": bdev_stats.get("bytes_written", 0),
                        "ticks": ticks,
                        "tick_rate": tick_rate
                    }
            
            # 如果没有找到指定的Bdev
            if log_errors:
                logging.error(f"Bdev '{bdev_name}' not found in RPC iostat output. Retrying...")
            # 短暂休眠后重试
            time.sleep(0.5)
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            # 捕获子进程执行失败或JSON解析失败的异常
            if log_errors:
                logging.warning(f"RPC call failed. Retrying...")
            # 短暂休眠后重试
            time.sleep(0.5)
        except Exception as e:
            # 捕获所有其他意外异常
            if log_errors:
                logging.error(f"An unexpected error occurred during RPC call: {e}")
            break # 出现意外错误则退出循环

    # 如果重试超时仍未成功，记录错误并返回None
    if log_errors:
        logging.error(f"Failed to get Bdev stats after {rpc_timeout} seconds of retries. Exiting...")
    return None


def main():
    """
    脚本主函数
    """
    # 在开始前再次检查RPC脚本路径
    if not os.path.exists(RPC_SCRIPT_PATH):
        sys.exit(f"ERROR: Please edit 'RPC_SCRIPT_PATH' in this script to point to your 'rpc.py'.")

    logging.info("Starting Bdev I/O monitoring script.")
    logging.info(f"Monitoring '{BDEV_NAME}' and writing data to '{OUTPUT_FILE}'.")

    # 获取初始I/O统计数据，使用较长的超时时间
    logging.info(f"Attempting to get initial Bdev stats (timeout: {INITIAL_RPC_TIMEOUT_S}s)...")
    last_stats = get_bdev_iostat(SOCK_PATH, BDEV_NAME, rpc_timeout=INITIAL_RPC_TIMEOUT_S, log_errors=False)
    if not last_stats:
        # 如果初始获取失败，则退出脚本
        sys.exit("Could not get initial Bdev stats. Exiting.")
    logging.info("Successfully find RPC socket and got initial Bdev stats.")
    # 记录初始的读写字节数和ticks
    last_bytes_read = last_stats['bytes_read']
    last_bytes_written = last_stats['bytes_written']
    last_ticks = last_stats['ticks']
    
    # 记录脚本开始时间
    start_time = time.perf_counter()

    try:
        # 使用'w'模式打开文件，写入CSV头
        with open(OUTPUT_FILE, 'w') as f:
            # 写入标题行，并使用固定宽度对齐
            f.write(f"{'Elapsed_Time_s(float)':<20}{'Read_BW_MB/s(float)':<20}{'Write_BW_MB/s(float)':<20}\n")
            # 无限循环，进行持续监控
            while True:
                # 轮询间隔休眠
                time.sleep(POLL_INTERVAL_S)

                # 获取当前I/O统计数据
                current_stats = get_bdev_iostat(SOCK_PATH, BDEV_NAME, rpc_timeout=POLL_RPC_TIMEOUT_S, log_errors=True)

                if current_stats is None:
                    # 如果RPC调用失败，记录错误并退出循环
                    logging.error("Lost connection to SPDK RPC socket. Exiting monitoring.")
                    break # RPC失败时退出循环

                # 计算两次统计之间的ticks差
                current_ticks = current_stats['ticks']
                ticks_rate = current_stats['tick_rate']
                delta_ticks = current_ticks - last_ticks
                
                # 计算时间间隔（秒），防止除以0
                delta_time_s = delta_ticks / ticks_rate if ticks_rate > 0 else 0
                
                # 仅在有时间变化时计算和记录
                if delta_time_s > 0:
                    # 计算两次统计之间的读写字节数差
                    delta_bytes_read = current_stats['bytes_read'] - last_bytes_read
                    delta_bytes_written = current_stats['bytes_written'] - last_bytes_written
                    
                    # 计算读写带宽（MB/s）
                    # 1024 * 1024 转换为兆字节
                    read_bw_mbs = (delta_bytes_read / (1024 * 1024)) / delta_time_s
                    write_bw_mbs = (delta_bytes_written / (1024 * 1024)) / delta_time_s
                    
                    # 写入数据行，使用固定宽度对齐并保留两位小数
                    elapsed_time = time.perf_counter() - start_time
                    f.write(f"{elapsed_time:<20.2f}{read_bw_mbs:<20.2f}{write_bw_mbs:<20.2f}\n")
                    f.flush() # 立即将数据写入文件
                
                # 更新上一次的读写字节数和ticks，为下一次循环做准备
                last_bytes_read = current_stats['bytes_read']
                last_bytes_written = current_stats['bytes_written']
                last_ticks = current_ticks

    except KeyboardInterrupt:
        # 捕获Ctrl+C中断，优雅地退出脚本
        logging.info("Monitoring stopped by user.")


if __name__ == "__main__":
    # 如果脚本作为主程序运行，则调用main函数
    main()
