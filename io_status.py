import os
import sys
import logging
import time
import json
import socket

# ==============================================================================
# 全局静态配置区域
# ==============================================================================
# BDEV_NAME = "Nvme0n1" 
BDEV_NAME = "Raid0"
SOCK_PATH = "/var/tmp/spdk_micro_test.sock"
# SOCK_PATH = "/var/tmp/spdk_test_api.sock"
# SOCK_PATH = "/var/tmp/spdk_kvcache.sock"
OUTPUT_FILE = "tmp.txt" # 输出文件，恢复为 .txt 格式
POLL_INTERVAL_S = 0.1   # 轮询间隔（秒）
# ==============================================================================

# 配置日志记录器
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

class SpdkRpcClient:
    """一个通过原生Socket与SPDK RPC服务器通信的客户端。"""
    def __init__(self, sock_path: str):
        if not os.path.exists(sock_path):
            raise FileNotFoundError(f"SPDK RPC socket not found at: {sock_path}")
        self.sock_path = sock_path
        self.sock = None
        self.request_id = 1

    def _connect(self):
        """建立到Unix-domain socket的连接。"""
        try:
            self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.sock.connect(self.sock_path)
        except (ConnectionRefusedError, FileNotFoundError) as e:
            logging.error(f"Failed to connect to SPDK socket '{self.sock_path}': {e}")
            self.sock = None
            raise

    def _send_request(self, method: str, params: dict = None) -> dict | None:
        """发送JSON-RPC请求并获取响应。"""
        if not self.sock:
            try:
                self._connect()
            except Exception:
                return None

        request = {
            "jsonrpc": "2.0",
            "method": method,
            "id": self.request_id,
        }
        if params:
            request["params"] = params
        
        try:
            self.sock.sendall(json.dumps(request).encode('utf-8'))
            self.request_id += 1
            
            response_data = self.sock.recv(8192 * 4).decode('utf-8')
            response_json = response_data.split('\n')[0]
            return json.loads(response_json)
        except (BrokenPipeError, ConnectionResetError, json.JSONDecodeError) as e:
            logging.warning(f"RPC communication error: {e}. Reconnecting...")
            self.close()
            return None
        except Exception as e:
            logging.error(f"An unexpected RPC error occurred: {e}")
            self.close()
            return None

    def get_bdev_iostat(self, bdev_name: str = None) -> dict | None:
        """获取bdev的iostat信息。"""
        params = {}
        if bdev_name:
            params['name'] = bdev_name
        
        response = self._send_request("bdev_get_iostat", params)
        
        if response and "result" in response:
            return response["result"]
        
        if response and "error" in response:
            logging.error(f"RPC error: {response['error']['message']}")

        return None

    def close(self):
        """关闭socket连接。"""
        if self.sock:
            self.sock.close()
            self.sock = None


def monitor_bdev(bdev_name: str, sock_path: str, interval: float, output_file: str):
    """主监控循环函数。"""
    logging.info(f"Connecting to SPDK RPC at '{sock_path}'...")
    try:
        rpc_client = SpdkRpcClient(sock_path)
    except FileNotFoundError:
        sys.exit(1)

    logging.info(f"Attempting to get initial stats for Bdev '{bdev_name}'...")
    initial_data = rpc_client.get_bdev_iostat()
    if not initial_data or not any(b.get("name") == bdev_name for b in initial_data.get("bdevs", [])):
        logging.error(f"Could not get initial stats or Bdev '{bdev_name}' not found. Exiting.")
        rpc_client.close()
        return

    ticks_rate = initial_data.get("tick_rate", 0)
    if ticks_rate == 0:
        logging.error("tick_rate is 0, cannot calculate bandwidth. Exiting.")
        rpc_client.close()
        return

    last_stats = next((bdev for bdev in initial_data["bdevs"] if bdev["name"] == bdev_name), None)
    
    last_ticks = initial_data.get("ticks", 0)
    last_bytes_read = last_stats.get("bytes_read", 0)
    last_bytes_written = last_stats.get("bytes_written", 0)

    logging.info(f"Successfully got initial stats. Starting monitoring and writing to '{output_file}'.")
    start_time = time.perf_counter()

    try:
        with open(output_file, 'w') as f:
            # 写入与原始脚本完全一致的标题行
            f.write(f"{'Elapsed_Time_s':<20}{'Read_BW_MiB/s':<20}{'Write_BW_MiB/s':<20}\n")

            while True:
                time.sleep(interval)

                all_stats = rpc_client.get_bdev_iostat()
                if not all_stats:
                    logging.error("Failed to get I/O stats from SPDK. Retrying...")
                    continue

                current_stats = next((bdev for bdev in all_stats.get("bdevs", []) if bdev["name"] == bdev_name), None)
                if not current_stats:
                    logging.warning(f"Bdev '{bdev_name}' not found in current stats. Skipping.")
                    continue
                
                current_ticks = all_stats.get("ticks", 0)
                
                if current_ticks < last_ticks:
                    logging.warning("Ticks counter may have wrapped around or reset. Skipping one interval.")
                    last_ticks = current_ticks
                    continue

                delta_ticks = current_ticks - last_ticks
                delta_time_s = delta_ticks / ticks_rate

                if delta_time_s > 0:
                    delta_bytes_read = current_stats['bytes_read'] - last_bytes_read
                    delta_bytes_written = current_stats['bytes_written'] - last_bytes_written
                    read_bw_mbs = (delta_bytes_read / (1024 * 1024)) / delta_time_s
                    write_bw_mbs = (delta_bytes_written / (1024 * 1024)) / delta_time_s
                    
                    elapsed_time = time.perf_counter() - start_time
                    
                    # 写入与原始脚本完全一致的数据行格式
                    f.write(f"{elapsed_time:<20.2f}{read_bw_mbs:<20.2f}{write_bw_mbs:<20.2f}\n")
                    f.flush()

                last_bytes_read = current_stats['bytes_read']
                last_bytes_written = current_stats['bytes_written']
                last_ticks = current_ticks

    except KeyboardInterrupt:
        logging.info("Monitoring stopped by user.")
    finally:
        rpc_client.close()
        logging.info("RPC client closed.")


def main():
    """
    脚本主函数，使用全局静态常量进行配置
    """
    monitor_bdev(
        bdev_name=BDEV_NAME,
        sock_path=SOCK_PATH,
        interval=POLL_INTERVAL_S,
        output_file=OUTPUT_FILE
    )


if __name__ == "__main__":
    main()