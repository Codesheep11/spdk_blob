#!/bin/bash

# =============================================================================
# setup_node0_hugepages.sh
#
# 功能:
#   1. 将所有 NUMA 节点的 2MB hugepage 分配清零。
#   2. 专门为 node0 分配指定数量的 2MB hugepage。
#   3. 验证分配结果。
#
# 注意: 需要 root 权限 (sudo) 运行此脚本。
# =============================================================================

# --- 配置 ---
NODE_TARGET="node1"
PAGES_TO_ALLOCATE=5120
PAGE_SIZE_KB="2048kB"
# --- 结束配置 ---

PAGE_SIZE_PATH="hugepages/hugepages-${PAGE_SIZE_KB}/nr_hugepages"
NODE_SYSFS_PATH="/sys/devices/system/node"

# 检查是否以 root 权限运行
if [ "$(id -u)" -ne 0 ]; then
  echo "错误: 此脚本必须以 root (sudo) 权限运行。" >&2
  exit 1
fi

echo "正在重置所有节点的 ${PAGE_SIZE_KB} hugepage 分配..."

# 1. 遍历所有节点 (node*) 并清零
for node_path in ${NODE_SYSFS_PATH}/node[0-9]*; do
    if [ -d "$node_path" ]; then
        node_name=$(basename "$node_path")
        hugepage_file="${node_path}/${PAGE_SIZE_PATH}"

        if [ -f "$hugepage_file" ]; then
            echo "  > 正在清零 $node_name..."
            # 写入 0 来释放/重置
            echo 0 > "$hugepage_file"
        else
            echo "  > 警告: 在 $node_name 上未找到 ${PAGE_SIZE_PATH}。跳过。"
        fi
    fi
done

echo "重置完成。"
echo ""
echo "正在为 ${NODE_TARGET} 分配 ${PAGES_TO_ALLOCATE} 个 ${PAGE_SIZE_KB} 页面..."

# 2. 为 node0 分配指定数量
NODE0_FILE="${NODE_SYSFS_PATH}/${NODE_TARGET}/${PAGE_SIZE_PATH}"
if [ ! -f "$NODE0_FILE" ]; then
    echo "错误: 找不到目标路径 ${NODE0_FILE}。" >&2
    exit 1
fi

echo ${PAGES_TO_ALLOCATE} > "$NODE0_FILE"

# 分配可能需要一点时间
sleep 1

echo "分配请求已发送。"
echo ""
echo "--- 验证结果 ---"

# 3. 验证
echo "A. 检查系统总览 (/proc/meminfo):"
grep Huge /proc/meminfo

echo ""
echo "B. 检查所有节点的具体分配:"
total_allocated=0
for node_path in ${NODE_SYSFS_PATH}/node[0-9]*; do
    if [ -d "$node_path" ]; then
        node_name=$(basename "$node_path")
        hugepage_file="${node_path}/${PAGE_SIZE_PATH}"
        if [ -f "$hugepage_file" ]; then
            count=$(cat "$hugepage_file")
            echo "  > $node_name: $count 页面"
            total_allocated=$((total_allocated + count))
        fi
    fi
done

echo ""
echo "脚本验证的总分配页面: $total_allocated"

if [ "$total_allocated" -eq "$PAGES_TO_ALLOCATE" ]; then
    echo "成功: 总分配数 (${total_allocated}) 与请求数 (${PAGES_TO_ALLOCATE}) 一致。"
else
    echo "警告: 总分配数 (${total_allocated}) 与请求数 (${PAGES_TO_ALLOCATE}) 不符！"
    echo "这通常意味着 ${NODE_TARGET} 没有足够的可用连续内存。"
fi