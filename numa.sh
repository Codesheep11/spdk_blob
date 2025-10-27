#!/bin/bash
for node in /sys/devices/system/node/node[0-9]*; do
    echo "--- $(basename $node) ---"
    for size_dir in $node/hugepages/hugepages-*; do
        if [ -d "$size_dir" ]; then
            size=$(basename $size_dir | cut -d- -f2)
            total=$(cat $size_dir/nr_hugepages)
            free=$(cat $size_dir/free_hugepages)
            used=$((total - free))
            
            echo "  HugePage Size: $size"
            echo "    Total: $total"
            echo "    Free:  $free"
            echo "    Used:  $used"
        fi
    done
done