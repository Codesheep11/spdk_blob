#!/bin/bash
rm -rf spdk_blob.egg-info
rm -rf build
rm -rf spdk_controller/__pycache__
rm spdk_blob.cpython-311-x86_64-linux-gnu.so
pip install -e . -v
echo "安装完成~"