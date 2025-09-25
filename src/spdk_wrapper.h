#ifndef SPDK_WRAPPER_H
#define SPDK_WRAPPER_H

#include <Python.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "spdk/blob.h"
#include "spdk/queue.h"
#include "spdk/thread.h"

#ifdef __cplusplus
extern "C"
{
#endif

    // --- 类型定义 ---
    typedef uint64_t spdk_blob_id;
#define C_API_INVALID_BLOB_ID ((spdk_blob_id) - 1)
    typedef void *spdk_blob_handle;

    struct worker_thread_s;

    typedef enum
    {
        RESULT_TYPE_NONE,
        RESULT_TYPE_BLOB_ID,
        RESULT_TYPE_BLOB_HANDLE
    } result_type_t;

    typedef struct
    {
        char *bdev_name;
        char *json_config_file;
        char *reactor_mask;
        char *sock;
    } spdk_thread_args_t;

    typedef struct spdk_async_ctx_s
    {
        union
        {
            int num_clusters;
            spdk_blob_id blobid;
            spdk_blob_handle handle;
        } data;
        PyObject *future;
        struct spdk_async_ctx_s *next;
    } spdk_async_ctx;

    typedef struct batch_io_context
    {
        PyObject *future;
        atomic_int ref_count;
        atomic_int first_errno;
    } batch_io_context_t;

    typedef struct io_request_context
    {
        PyObject *future;
        spdk_blob_handle handle;
        void *payload;
        uint64_t offset_io_units;
        uint64_t num_io_units;
        bool is_read;
        struct worker_thread_s *worker;
        TAILQ_ENTRY(io_request_context)
        link;
        batch_io_context_t *batch_ctx;
    } io_request_context_t;

    typedef struct
    {
        sem_t sem;
        uint64_t *size_out;
    } msg_ctx_get_size;

    typedef struct
    {
        sem_t sem;
        union
        {
            struct
            {
                size_t size_bytes;
                void **buffer_out;
            } alloc;
            struct
            {
                void *buffer;
            } free;
        };
    } msg_ctx_buffer_op;

    typedef struct worker_thread_s
    {
        struct spdk_thread *thread;
        struct spdk_io_channel *channel;
        uint32_t lcore;
        uint32_t inflight_io;
        TAILQ_HEAD(, io_request_context)
        pending_py_requests;
        struct spdk_poller *io_poller;
        io_request_context_t *io_req_freelist;
    } worker_thread_t;

    // 统一的 I/O 消息结构体，用于单次和批量提交
    typedef struct spdk_io_msg
    {
        io_request_context_t **contexts; // 指向 I/O 请求上下文指针的数组
        uint32_t count;                  // 数组中的请求数量 (单次IO时为1)
        batch_io_context_t *batch_ctx;   // 指向批次共享上下文。对于单次IO，此指针为 NULL。
    } spdk_io_msg_t;

    // --- 公开 API 函数声明 ---

    // --- 生命周期管理 ---
    int c_api_init(const char *bdev_name, const char *json_config_file, const char *reactor_mask, const char *sock, PyObject *completion_loop);
    int c_api_unload(void);

    // --- 线程管理 API ---
    int c_api_register_io_thread(void);
    void c_api_unregister_io_thread(void);

    // --- 异步元数据 API ---
    int c_api_create_async(int num_clusters, PyObject *future);
    int c_api_delete_async(spdk_blob_id blobid, PyObject *future);
    int c_api_open_blob_async(spdk_blob_id blobid, PyObject *future);
    int c_api_close_blob_async(spdk_blob_handle handle, PyObject *future);

    // --- 异步 I/O API ---
    io_request_context_t *c_api_alloc_io_request_ctx(int worker_id);
    void c_api_free_io_request_ctx(int worker_id, io_request_context_t *ctx);

    int c_api_submit_single_io_async(int worker_id, io_request_context_t *ctx, PyObject *future);
    int c_api_submit_batch_io_async(int worker_id, io_request_context_t **contexts, uint32_t count, PyObject *future);

    // --- 非I/O辅助函数 ---
    uint64_t c_api_get_page_size(void);
    uint64_t c_api_get_cluster_size(void);
    uint64_t c_api_get_free_clusters(void);
    uint64_t c_api_get_io_unit_size(void);
    void *c_api_alloc_io_buffer(size_t size_bytes);
    void c_api_free_io_buffer(void *buffer);

#ifdef __cplusplus
}
#endif

#endif // SPDK_WRAPPER_H