import ctypes

# Constants
BLOB_SIZE_IN_BYTES_DEFAULT = -1
INITIAL_BLOB_COUNT = 8192
MAX_TOTAL_BLOB_COUNT = 8192 * 5
BLOB_REPOPULATE_BATCH_SIZE = INITIAL_BLOB_COUNT // 4
LOW_WATER_MARK = INITIAL_BLOB_COUNT // 4

class C_IO_Request(ctypes.Structure):
    
    _fields_ = [
        ("handle", ctypes.c_void_p),      # spdk_blob_handle (void*)
        ("payload", ctypes.c_void_p),     # void*
        ("offset_units", ctypes.c_uint64),
        ("num_units", ctypes.c_uint64),
    ]