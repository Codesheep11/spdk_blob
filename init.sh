#!/bin/bash

# =============================================================================
# setup_multi_node_hugepages.sh
#
# 功能:
#   1. 将所有 NUMA 节点的 2MB hugepage 分配清零。
#   2. 根据配置，为指定的 NUMA 节点分配指定数量的 2MB hugepage。
#   3. 验证分配结果。
#
# 注意: 需要 root 权限 (sudo) 运行此脚本。
# =============================================================================

# --- 配置 ---
declare -A NODE_ALLOCATIONS
NODE_ALLOCATIONS=(
    ["node0"]=0
    ["node1"]=8192
)

PAGE_SIZE_KB="2048kB"

PAGE_SIZE_PATH="hugepages/hugepages-${PAGE_SIZE_KB}/nr_hugepages"
NODE_SYSFS_PATH="/sys/devices/system/node"

# 检查是否以 root 权限运行
if [ "$(id -u)" -ne 0 ]; then
  echo "错误: 此脚本必须以 root (sudo) 权限运行。" >&2
  exit 1
fi

echo "正在重置所有节点的 ${PAGE_SIZE_KB} hugepage 分配..."

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
echo "正在根据配置分配 ${PAGE_SIZE_KB} 页面..."

# 2. 遍历配置并为每个指定节点分配
total_pages_requested=0
for node_name in "${!NODE_ALLOCATIONS[@]}"; do
    pages_to_allocate="${NODE_ALLOCATIONS[$node_name]}"
    total_pages_requested=$((total_pages_requested + pages_to_allocate))
    
    node_file="${NODE_SYSFS_PATH}/${node_name}/${PAGE_SIZE_PATH}"
    
    if [ -f "$node_file" ]; then
        echo "  > 正在为 $node_name 分配 $pages_to_allocate 页面..."
        echo ${pages_to_allocate} > "$node_file"
    else
        echo "  > 错误: 找不到目标路径 ${node_file}。跳过 $node_name。" >&2
    fi
done

# 分配可能需要一点时间
sleep 1

echo "所有分配请求已发送。"
echo ""
echo "--- 验证结果 ---"

# 3. 验证
echo "A. 检查系统总览 (/proc/meminfo):"
grep Huge /proc/meminfo

echo ""
echo "B. 检查所有节点的具体分配:"
total_allocated_actual=0
for node_path in ${NODE_SYSFS_PATH}/node[0-9]*; do
    if [ -d "$node_path" ]; then
        node_name=$(basename "$node_path")
        hugepage_file="${node_path}/${PAGE_SIZE_PATH}"
        if [ -f "$hugepage_file" ]; then
            # 读取实际分配到的数量
            count=$(cat "$hugepage_file")
            # 从关联数组中获取请求的数量 (如果未定义则为 0)
            requested_count=${NODE_ALLOCATIONS[$node_name]:-0}
            
            echo "  > $node_name: $count 页面 (请求: $requested_count)"
            total_allocated_actual=$((total_allocated_actual + count))
        fi
    fi
done

echo ""
echo "脚本验证的实际总分配页面: $total_allocated_actual"
echo "脚本配置的总请求页面: $total_pages_requested"

if [ "$total_allocated_actual" -eq "$total_pages_requested" ]; then
    echo "成功: 实际总分配数 (${total_allocated_actual}) 与总请求数 (${total_pages_requested}) 一致。"
else
    echo "警告: 实际总分配数 (${total_allocated_actual}) 与总请求数 (${total_pages_requested}) 不符！"
    echo "这通常意味着一个或多个节点没有足够的可用连续内存。"
fi