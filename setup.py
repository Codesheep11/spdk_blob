from pathlib import Path
import os
import shlex
import subprocess
from setuptools import setup, Extension

SPDK_BUILD = Path(os.environ.get("SPDK_BUILD", "/home/yangxc/project/KVCache/spdk/build")).resolve()
SPDK_ROOT = SPDK_BUILD.parent # SPDK 源码根目录
DPDK_LIB   = SPDK_BUILD.parent / "dpdk" / "build" / "lib"
ISA_L      = SPDK_BUILD.parent / "isa-l" / ".libs"
ISA_L_CRY  = SPDK_BUILD.parent / "isa-l-crypto" / ".libs"

# ---------- 编译 / 链接公共参数 ----------
common_cflags = [
    "-Wall", "-Wextra", "-Wno-unused-parameter", "-Wno-missing-field-initializers",
    "-Wmissing-declarations", "-Wno-pointer-sign", "-Wstrict-prototypes",
    "-Wold-style-definition", "-fno-strict-aliasing", "-fPIC",
    "-fstack-protector", "-fno-common", "-march=native", "-mno-avx512f",
    "-O2", "-g", "-std=gnu11", "-D_GNU_SOURCE", "-DNDEBUG",
]
optimization_flags = [
    "-O3",
    "-march=native", 
    "-flto", 
    "-fno-semantic-interposition",
    "-ffast-math",
]

common_cflags = optimization_flags + common_cflags

common_ldflags = [
    "-fuse-ld=bfd", "-Wl,-z,relro,-z,now", "-Wl,-z,noexecstack",
]

# RPATH：运行时自动定位 SPDK / DPDK / ISA-L 库
rpath_flags = [
    f"-Wl,-rpath,{SPDK_BUILD / 'lib'}",
    f"-Wl,-rpath,{SPDK_BUILD / 'bin/spdk_tgt'}",
    f"-Wl,-rpath,{DPDK_LIB}",
    f"-Wl,-rpath,{ISA_L}",
    f"-Wl,-rpath,{ISA_L_CRY}",
]

# ---------- 手动添加 SPDK 的 CFLAGS 和 LDFLAGS ----------
# 手动指定 SPDK 的头文件路径
SPDK_CFLAGS = [
    f"-I{SPDK_ROOT / 'include'}",
    f"-I{SPDK_ROOT / 'lib'}",
    f"-I{SPDK_ROOT / 'lib' / 'event'}",
    f"-I{SPDK_BUILD / 'include'}",
]

SPDK_LDFLAGS = [
    f"-L{SPDK_BUILD / 'lib'}",
]

spdk_libs = [
    "-Wl,--no-as-needed",           
    "-Wl,--whole-archive",
    "-lspdk_bdev_malloc", "-lspdk_bdev_null", "-lspdk_bdev_nvme",
    "-lspdk_bdev_passthru", "-lspdk_bdev_lvol", "-lspdk_bdev_raid",
    "-lspdk_bdev_error", "-lspdk_bdev_gpt", "-lspdk_bdev_split",
    "-lspdk_bdev_delay", "-lspdk_bdev_zone_block",
    "-lspdk_blob_bdev",
    "-lspdk_lvol", "-lspdk_blob", "-lspdk_nvme", "-lspdk_bdev_aio",
    "-lspdk_bdev_ftl", "-lspdk_ftl", "-lspdk_bdev_virtio", "-lspdk_virtio",
    "-lspdk_vfio_user", "-lspdk_accel_error", "-lspdk_accel_ioat",
    "-lspdk_ioat", "-lspdk_scheduler_dynamic", "-lspdk_env_dpdk",
    "-lspdk_scheduler_dpdk_governor", "-lspdk_scheduler_gscheduler",
    "-lspdk_sock_posix", "-lspdk_keyring_file", "-lspdk_keyring_linux",
    "-lspdk_fsdev_aio", "-lspdk_fsdev", "-lspdk_event", "-lspdk_event_bdev",
    "-lspdk_bdev", "-lspdk_notify", "-lspdk_event_accel", "-lspdk_accel",
    "-lspdk_dma", "-lspdk_event_vmd", "-lspdk_vmd", "-lspdk_event_sock",
    "-lspdk_sock", "-lspdk_event_iobuf", "-lspdk_event_keyring",
    "-lspdk_init", "-lspdk_thread", "-lspdk_trace", "-lspdk_keyring",
    "-lspdk_rpc", "-lspdk_jsonrpc", "-lspdk_json",
    "-lspdk_util", "-lspdk_log",
    "-lisal","-lisal_crypto",
    "-Wl,--no-whole-archive",
    "-Wl,--as-needed",
]

dpdk_so = [
    "-Wl,--no-as-needed",           
    "-Wl,--whole-archive",
    str(DPDK_LIB / "librte_bus_pci.so"),
    str(DPDK_LIB / "librte_cryptodev.so"),
    str(DPDK_LIB / "librte_dmadev.so"),
    str(DPDK_LIB / "librte_eal.so"),
    str(DPDK_LIB / "librte_ethdev.so"),
    str(DPDK_LIB / "librte_hash.so"),
    str(DPDK_LIB / "librte_kvargs.so"),
    str(DPDK_LIB / "librte_log.so"),
    str(DPDK_LIB / "librte_mbuf.so"),
    str(DPDK_LIB / "librte_mempool.so"),
    str(DPDK_LIB / "librte_mempool_ring.so"),
    str(DPDK_LIB / "librte_net.so"),
    str(DPDK_LIB / "librte_pci.so"),
    str(DPDK_LIB / "librte_power.so"),
    str(DPDK_LIB / "librte_power_acpi.so"),
    str(DPDK_LIB / "librte_power_amd_pstate.so"),
    str(DPDK_LIB / "librte_power_cppc.so"),
    str(DPDK_LIB / "librte_power_intel_pstate.so"),
    str(DPDK_LIB / "librte_power_intel_uncore.so"),
    str(DPDK_LIB / "librte_power_kvm_vm.so"),
    str(DPDK_LIB / "librte_rcu.so"),
    str(DPDK_LIB / "librte_ring.so"),
    str(DPDK_LIB / "librte_telemetry.so"),
    str(DPDK_LIB / "librte_vhost.so"),
    "-Wl,--no-whole-archive",
    "-Wl,--as-needed",
]

third_party_libs = [
    "-Wl,--no-as-needed",           
    "-Wl,--whole-archive",  
    f"-L{ISA_L}",           
    f"-L{ISA_L_CRY}", 
    "-lisal",
    "-lisal_crypto",
    "-pthread", "-lrt", "-luuid", "-lssl", "-lcrypto",
    "-lm", "-llz4", "-lfuse3", "-lkeyutils", "-laio",
    "-Wl,--no-whole-archive",
    "-Wl,--as-needed",
]

# ---------- Extension ----------
module = Extension(
    "spdk_blob",                  
    sources=["src/py_wrapper.c",
             "src/spdk_wrapper.c"
             ],   
    extra_compile_args=common_cflags + SPDK_CFLAGS,
    extra_link_args=(
        common_ldflags
        + rpath_flags
        + SPDK_LDFLAGS 
        + spdk_libs
        + dpdk_so
        + third_party_libs
    ),
    language="c",
)

# ---------- setup() ----------
setup(
    name="spdk_blob",
    version="0.1.0",
    description="Python bindings for SPDK blobstore",
    ext_modules=[module],
    python_requires=">=3.8",
)