#include "spdk_wrapper.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "spdk/bdev.h"
#include "spdk/blob_bdev.h"
#include "spdk/env.h"
#include "spdk/event.h"
#include "spdk/log.h"
#include "spdk/util.h"

// #define SPDK_API_DEBUG 1
#ifdef SPDK_API_DEBUG
#define DEBUG_LOG(fmt, ...)                                                                            \
    do                                                                                                 \
    {                                                                                                  \
        fprintf(stdout, "[TRACE] %s:%d:%s(): " fmt "\n", __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
        fflush(stdout);                                                                                \
    } while (0)
#else
#define DEBUG_LOG(fmt, ...) \
    do                      \
    {                       \
    } while (0)
#endif

#define ASYNC_CTX_POOL_SIZE 256
#define IO_REQUEST_POOL_SIZE 4096
#define MAX_PY_THREADS 128
#define CACHE_LINE_SIZE 64

// --- 静态函数声明 ---
static void _finish_async_op(PyObject *future, result_type_t rtype, uint64_t rval, int bserrno);
static worker_thread_t *get_current_worker(void);

// --- 全局状态 ---
struct spdk_blob_store *g_bs = NULL;
static worker_thread_t *g_worker_threads = NULL;
static uint32_t g_num_worker_threads = 0;
static struct spdk_thread *g_main_spdk_thread = NULL;
static PyObject *g_completion_loop = NULL;

static uint64_t g_cached_page_size = 0;
static uint64_t g_cached_cluster_size = 0;
static uint64_t g_cached_io_unit_size = 0;

volatile int g_async_ops_in_flight = 0;
static volatile bool g_shutdown_initiated = false;
static pthread_t g_spdk_thread_id;
static volatile int g_spdk_thread_rc = 0;
static volatile bool g_spdk_ready = false;
static volatile bool g_spdk_init_failed = false;
static struct spdk_poller *g_unload_poller = NULL;
static pthread_cond_t g_init_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t g_workers_ready_sem;

static struct
{
    pthread_mutex_t lock;
    spdk_async_ctx *freelist;
    void *pool_buffer;
} g_async_ctx_pool;

static struct
{
    void *pool_buffer;
} g_io_request_pool;

// --- 对象池和工具函数 ---
static int init_async_ctx_pool(void);
static void destroy_async_ctx_pool(void);
static spdk_async_ctx *alloc_async_ctx(void);
static void free_async_ctx(spdk_async_ctx *ctx);

static int init_io_request_pool(uint32_t num_workers);
static void destroy_io_request_pool(void);
static io_request_context_t *alloc_io_request_ctx(worker_thread_t *worker);
static void free_io_request_ctx(io_request_context_t *req_ctx, worker_thread_t *worker);
static void _sem_destroy_wrapper(sem_t *sem);

// --- 回调函数 ---
static void _io_async_cb(void *cb_arg, int bserrno)
{
    io_request_context_t *req_ctx = cb_arg;
    worker_thread_t *worker = req_ctx->worker;

    if (req_ctx->batch_ctx)
    {
        batch_io_context_t *batch = req_ctx->batch_ctx;
        if (bserrno != 0)
        {
            int expected_errno = 0;
            atomic_compare_exchange_strong(&batch->first_errno, &expected_errno, bserrno);
        }

        if (atomic_fetch_sub(&batch->ref_count, 1) == 1)
        {
            int final_errno = atomic_load(&batch->first_errno);
            __sync_fetch_and_sub(&g_async_ops_in_flight, 1);
            _finish_async_op(batch->future, RESULT_TYPE_NONE, 0, final_errno);
            Py_DECREF(batch->future);
            free(batch);
        }
    }
    else
    {
        __sync_fetch_and_sub(&g_async_ops_in_flight, 1);
        _finish_async_op(req_ctx->future, RESULT_TYPE_NONE, 0, bserrno);
        Py_DECREF(req_ctx->future);
    }
    if (worker)
    {
        free_io_request_ctx(req_ctx, worker);
    }
}
static void _generic_async_cb(void *cb_arg, int bserrno)
{
    spdk_async_ctx *ctx = cb_arg;
    __sync_fetch_and_sub(&g_async_ops_in_flight, 1);
    _finish_async_op(ctx->future, RESULT_TYPE_NONE, 0, bserrno);
    Py_DECREF(ctx->future);
    free_async_ctx(ctx);
}
static void _create_blob_cb(void *cb_arg, spdk_blob_id blob_id, int bserrno)
{
    spdk_async_ctx *ctx = cb_arg;
    __sync_fetch_and_sub(&g_async_ops_in_flight, 1);
    _finish_async_op(ctx->future, RESULT_TYPE_BLOB_ID, blob_id, bserrno);
    Py_DECREF(ctx->future);
    free_async_ctx(ctx);
}
static void _open_blob_cb(void *cb_arg, struct spdk_blob *blob, int bserrno)
{
    spdk_async_ctx *ctx = cb_arg;
    __sync_fetch_and_sub(&g_async_ops_in_flight, 1);
    _finish_async_op(ctx->future, RESULT_TYPE_BLOB_HANDLE, (uint64_t)blob, bserrno);
    Py_DECREF(ctx->future);
    free_async_ctx(ctx);
}
static worker_thread_t *get_current_worker(void)
{
    uint32_t current_lcore = spdk_env_get_current_core();
    for (uint32_t i = 0; i < g_num_worker_threads; i++)
    {
        if (g_worker_threads[i].lcore == current_lcore)
            return &g_worker_threads[i];
    }
    return NULL;
}
static int worker_io_poller(void *arg)
{
    worker_thread_t *worker = arg;
    io_request_context_t *req_ctx;

    while (!TAILQ_EMPTY(&worker->pending_py_requests))
    {
        req_ctx = TAILQ_FIRST(&worker->pending_py_requests);
        TAILQ_REMOVE(&worker->pending_py_requests, req_ctx, link);
        worker->inflight_io++;

        if (req_ctx->is_read)
        {
            // DEBUG_LOG("Issuing read: handle=%p, offset=%lu, length=%lu", req_ctx->handle, req_ctx->offset_io_units, req_ctx->num_io_units);
            spdk_blob_io_read(req_ctx->handle, worker->channel, req_ctx->payload, req_ctx->offset_io_units, req_ctx->num_io_units, _io_async_cb, req_ctx);
        }
        else
        {
            // DEBUG_LOG("Issuing write: handle=%p, offset=%lu, length=%lu", req_ctx->handle, req_ctx->offset_io_units, req_ctx->num_io_units);
            spdk_blob_io_write(req_ctx->handle, worker->channel, req_ctx->payload, req_ctx->offset_io_units, req_ctx->num_io_units, _io_async_cb, req_ctx);
        }
    }
    return SPDK_POLLER_BUSY;
}

// --- 消息处理函数 ---
static void _unified_enqueue_io_handler(void *arg)
{
    spdk_io_msg_t *msg = arg;
    worker_thread_t *worker = get_current_worker();

    if (worker)
    {
        for (uint32_t i = 0; i < msg->count; i++)
        {
            io_request_context_t *req_ctx = msg->contexts[i];
            req_ctx->batch_ctx = msg->batch_ctx;
            TAILQ_INSERT_TAIL(&worker->pending_py_requests, req_ctx, link);
        }
    }
    else
    {
        SPDK_ERRLOG("Cannot find worker on lcore %u to enqueue request\n", spdk_env_get_current_core());
        if (msg->batch_ctx)
        {
            _finish_async_op(msg->batch_ctx->future, RESULT_TYPE_NONE, 0, -EIO);
            Py_DECREF(msg->batch_ctx->future);
            free(msg->batch_ctx);
        }
        else if (msg->count > 0)
        {
            _finish_async_op(msg->contexts[0]->future, RESULT_TYPE_NONE, 0, -EIO);
            Py_DECREF(msg->contexts[0]->future);
        }
    }

    free(msg->contexts);
    free(msg);
}
static void _msg_create_blob(void *arg)
{
    spdk_async_ctx *ctx = arg;
    __sync_fetch_and_add(&g_async_ops_in_flight, 1);
    struct spdk_blob_opts opts;
    spdk_blob_opts_init(&opts, sizeof(opts));
    opts.num_clusters = ctx->data.num_clusters;
    spdk_bs_create_blob_ext(g_bs, &opts, _create_blob_cb, ctx);
}
static void _msg_delete_blob(void *arg)
{
    spdk_async_ctx *ctx = arg;
    __sync_fetch_and_add(&g_async_ops_in_flight, 1);
    spdk_bs_delete_blob(g_bs, ctx->data.blobid, _generic_async_cb, ctx);
}
static void _msg_open_blob(void *arg)
{
    spdk_async_ctx *ctx = arg;
    __sync_fetch_and_add(&g_async_ops_in_flight, 1);
    spdk_bs_open_blob(g_bs, ctx->data.blobid, _open_blob_cb, ctx);
}
static void _msg_close_blob(void *arg)
{
    spdk_async_ctx *ctx = arg;
    __sync_fetch_and_add(&g_async_ops_in_flight, 1);
    spdk_blob_close(ctx->data.handle, _generic_async_cb, ctx);
}
static void _msg_get_free_clusters(void *arg)
{
    msg_ctx_get_size *ctx = arg;
    *ctx->size_out = g_bs ? spdk_bs_free_cluster_count(g_bs) : 0;
    sem_post(&ctx->sem);
}
static void _msg_alloc_buffer(void *arg)
{
    msg_ctx_buffer_op *ctx = arg;
    *ctx->alloc.buffer_out = spdk_dma_zmalloc(ctx->alloc.size_bytes, 0, NULL);
    sem_post(&ctx->sem);
}
static void _msg_free_buffer(void *arg)
{
    msg_ctx_buffer_op *ctx = arg;
    spdk_dma_free(ctx->free.buffer);
    sem_post(&ctx->sem);
}

// --- SPDK 启动和关闭流程 ---
static void _worker_thread_init_msg(void *arg)
{
    worker_thread_t *worker = arg;
    DEBUG_LOG("Initializing worker on lcore %u.", worker->lcore);
    struct spdk_cpuset *cpumask = spdk_cpuset_alloc();
    if (!cpumask)
    {
        g_spdk_init_failed = true;
        sem_post(&g_workers_ready_sem);
        return;
    }
    spdk_cpuset_zero(cpumask);
    spdk_cpuset_set_cpu(cpumask, worker->lcore, true);
    spdk_thread_set_cpumask(cpumask);
    spdk_cpuset_free(cpumask);
    worker->channel = spdk_bs_alloc_io_channel(g_bs);
    if (!worker->channel)
    {
        SPDK_ERRLOG("Failed to alloc io channel on lcore %u\n", worker->lcore);
        g_spdk_init_failed = true;
    }
    TAILQ_INIT(&worker->pending_py_requests);
    worker->inflight_io = 0;

    uint32_t worker_idx = worker - g_worker_threads;

    io_request_context_t *my_req_pool_start = (io_request_context_t *)g_io_request_pool.pool_buffer + (worker_idx * IO_REQUEST_POOL_SIZE);
    worker->io_req_freelist = NULL;
    for (int i = 0; i < IO_REQUEST_POOL_SIZE; i++)
    {
        my_req_pool_start[i].link.tqe_next = (struct io_request_context *)worker->io_req_freelist;
        worker->io_req_freelist = &my_req_pool_start[i];
    }

    worker->io_poller = SPDK_POLLER_REGISTER(worker_io_poller, worker, 0);
    DEBUG_LOG("Worker on lcore %u is ready.", worker->lcore);
    sem_post(&g_workers_ready_sem);
}
static void bs_unload_cb(void *cb_arg, int bserrno)
{
    DEBUG_LOG("Blobstore unload complete, bserrno=%d.", bserrno);
    spdk_app_stop(bserrno);
}
static void _worker_cleanup_msg(void *arg)
{
    worker_thread_t *worker = get_current_worker();
    if (!worker)
        return;
    if (worker->io_poller)
        spdk_poller_unregister(&worker->io_poller);
    io_request_context_t *req_ctx, *tmp_req_ctx;
    TAILQ_FOREACH_SAFE(req_ctx, &worker->pending_py_requests, link, tmp_req_ctx)
    {
        TAILQ_REMOVE(&worker->pending_py_requests, req_ctx, link);
        _finish_async_op(req_ctx->future, RESULT_TYPE_NONE, 0, -ESHUTDOWN);
        Py_DECREF(req_ctx->future);
        free_io_request_ctx(req_ctx, worker);
    }
    if (worker->channel)
    {
        spdk_bs_free_io_channel(worker->channel);
        worker->channel = NULL;
    }
    spdk_thread_exit(worker->thread);
    sem_post((sem_t *)arg);
}
static int _unload_poller_cb(void *arg)
{
    if (g_async_ops_in_flight == 0)
    {
        DEBUG_LOG("All in-flight async ops completed. Proceeding with cleanup.");
        spdk_poller_unregister(&g_unload_poller);
        sem_t worker_cleanup_sem;
        sem_init(&worker_cleanup_sem, 0, 0);
        for (uint32_t i = 0; i < g_num_worker_threads; i++)
        {
            if (g_worker_threads[i].thread)
                spdk_thread_send_msg(g_worker_threads[i].thread, _worker_cleanup_msg, &worker_cleanup_sem);
        }
        for (uint32_t i = 0; i < g_num_worker_threads; i++)
            sem_wait(&worker_cleanup_sem);
        sem_destroy(&worker_cleanup_sem);
        spdk_bs_unload(g_bs, bs_unload_cb, NULL);
        return SPDK_POLLER_BUSY;
    }
    return SPDK_POLLER_BUSY;
}
static void _msg_shutdown(void *arg)
{
    if (g_shutdown_initiated)
        return;
    g_shutdown_initiated = true;
    g_unload_poller = SPDK_POLLER_REGISTER(_unload_poller_cb, NULL, 10000);
}
static void _check_workers_ready(void *arg)
{
    for (uint32_t i = 0; i < g_num_worker_threads; i++)
        sem_wait(&g_workers_ready_sem);
    sem_destroy(&g_workers_ready_sem);
    if (g_spdk_init_failed)
    {
        spdk_app_stop(-1);
        return;
    }
    pthread_mutex_lock(&g_init_mutex);
    g_spdk_ready = true;
    pthread_cond_signal(&g_init_cond);
    pthread_mutex_unlock(&g_init_mutex);
}
static void start_worker_threads(void)
{
    uint32_t i = 0;
    uint32_t lcore;
    uint32_t main_lcore = spdk_env_get_main_core();

    uint32_t num_workers_from_env = 0;
    SPDK_ENV_FOREACH_CORE(lcore)
    {
        if (lcore != main_lcore)
        {
            num_workers_from_env++;
        }
    }
    if (g_num_worker_threads != num_workers_from_env)
    {
        SPDK_ERRLOG("Worker count mismatch! Pre-calculated: %u, from env: %u\n", g_num_worker_threads, num_workers_from_env);
        g_spdk_init_failed = true;
        spdk_app_stop(-1);
        return;
    }

    if (g_num_worker_threads == 0)
    {
        SPDK_ERRLOG("No worker cores found, need at least one worker core.\n");
        g_spdk_init_failed = true;
        spdk_app_stop(-1);
        return;
    }

    size_t worker_alloc_size = g_num_worker_threads * sizeof(worker_thread_t);
    g_worker_threads = aligned_alloc(CACHE_LINE_SIZE, worker_alloc_size);
    if (!g_worker_threads)
    {
        g_spdk_init_failed = true;
        spdk_app_stop(-ENOMEM);
        return;
    }
    memset(g_worker_threads, 0, worker_alloc_size);

    sem_init(&g_workers_ready_sem, 0, 0);
    SPDK_ENV_FOREACH_CORE(lcore)
    {
        if (lcore == main_lcore)
            continue;
        worker_thread_t *worker = &g_worker_threads[i];
        worker->lcore = lcore;
        char thread_name[32];
        snprintf(thread_name, sizeof(thread_name), "worker_%u", lcore);
        worker->thread = spdk_thread_create(thread_name, NULL);
        if (!worker->thread)
        {
            g_spdk_init_failed = true;
            break;
        }
        spdk_thread_send_msg(worker->thread, _worker_thread_init_msg, worker);
        i++;
    }
    if (g_spdk_init_failed)
    {
        sem_destroy(&g_workers_ready_sem);
        spdk_app_stop(-1);
        return;
    }
    spdk_thread_send_msg(g_main_spdk_thread, _check_workers_ready, NULL);
}
static void bs_init_cb(void *cb_arg, struct spdk_blob_store *bs, int bserrno)
{
    if (bserrno != 0)
    {
        g_spdk_init_failed = true;
        spdk_app_stop(bserrno);
        return;
    }
    g_bs = bs;
    g_main_spdk_thread = spdk_get_thread();

    g_cached_page_size = spdk_bs_get_page_size(g_bs);
    g_cached_cluster_size = spdk_bs_get_cluster_size(g_bs);
    g_cached_io_unit_size = spdk_bs_get_io_unit_size(g_bs);
    DEBUG_LOG("Blobstore metadata cached: page_size=%lu, cluster_size=%lu, io_unit_size=%lu",
              g_cached_page_size, g_cached_cluster_size, g_cached_io_unit_size);

    start_worker_threads();
}
static void base_bdev_event_cb(enum spdk_bdev_event_type type, struct spdk_bdev *bdev, void *event_ctx) {}
static void spdk_app_main(void *arg1)
{
    spdk_thread_args_t *args = (spdk_thread_args_t *)arg1;
    struct spdk_bs_dev *bs_dev = NULL;
    int rc = spdk_bdev_create_bs_dev_ext(args->bdev_name, base_bdev_event_cb, NULL, &bs_dev);
    if (rc != 0)
    {
        SPDK_ERRLOG("Could not create blob bdev\n");
        g_spdk_init_failed = true;
        spdk_app_stop(-1);
        return;
    }
    struct spdk_bs_opts opts;
    spdk_bs_opts_init(&opts, sizeof(opts));
    spdk_bs_init(bs_dev, &opts, bs_init_cb, NULL);
}
static void *spdk_thread_func(void *arg)
{
    spdk_thread_args_t *args = (spdk_thread_args_t *)arg;
    struct spdk_app_opts opts;
    spdk_app_opts_init(&opts, sizeof(opts));
    opts.name = "spdk_wrapper_app";
    opts.json_config_file = args->json_config_file;
    opts.reactor_mask = args->reactor_mask;
    opts.rpc_addr = args->sock;
    g_spdk_thread_rc = spdk_app_start(&opts, spdk_app_main, args);
    free(args->bdev_name);
    free(args->json_config_file);
    free(args->reactor_mask);
    free(args->sock);
    free(args);
    if (g_spdk_thread_rc || !g_spdk_ready)
    {
        pthread_mutex_lock(&g_init_mutex);
        g_spdk_init_failed = true;
        pthread_cond_signal(&g_init_cond);
        pthread_mutex_unlock(&g_init_mutex);
    }
    return NULL;
}

// --- Python <-> C 交互 ---
static void _finish_async_op(PyObject *future, result_type_t rtype, uint64_t rval, int bserrno)
{
    if (!future || !g_completion_loop)
        return;

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject *call_soon = PyObject_GetAttrString(g_completion_loop, "call_soon_threadsafe");
    if (!call_soon)
    {
        PyErr_Print();
        goto cleanup;
    }

    if (bserrno == 0)
    {
        PyObject *result = NULL;
        switch (rtype)
        {
        case RESULT_TYPE_BLOB_ID:
            result = PyLong_FromUnsignedLongLong(rval);
            break;
        case RESULT_TYPE_BLOB_HANDLE:
            result = PyLong_FromVoidPtr((void *)rval);
            break;
        case RESULT_TYPE_NONE:
        default:
            result = Py_None;
            Py_INCREF(result);
            break;
        }

        if (result)
        {
            PyObject *set_result_func = PyObject_GetAttrString(future, "set_result");
            if (set_result_func)
            {
                PyObject_CallFunctionObjArgs(call_soon, set_result_func, result, NULL);
                Py_DECREF(set_result_func);
            }
            Py_DECREF(result);
        }
    }
    else
    {
        PyObject *set_exception_func = PyObject_GetAttrString(future, "set_exception");
        if (set_exception_func)
        {
            char err_msg[128];
            snprintf(err_msg, sizeof(err_msg), "SPDK op failed with errno %d", bserrno);
            PyObject *exc = PyObject_CallFunction(PyExc_OSError, "s", err_msg);
            PyObject_CallFunctionObjArgs(call_soon, set_exception_func, exc, NULL);
            Py_DECREF(exc);
            Py_DECREF(set_exception_func);
        }
    }

    Py_DECREF(call_soon);
cleanup:
    PyGILState_Release(gstate);
}
static uint32_t count_set_bits(unsigned long long n)
{
    uint32_t count = 0;
    while (n > 0)
    {
        n &= (n - 1);
        count++;
    }
    return count;
}

// --- 公开 API 实现 ---
uint32_t c_api_get_worker_count(void)
{
    return g_num_worker_threads;
}

int c_api_init(const char *bdev_name, const char *json_config_file, const char *reactor_mask, const char *sock, PyObject *completion_loop)
{
    if (g_spdk_thread_id != 0)
        return -1;

    unsigned long long mask_val = strtoull(reactor_mask, NULL, 16);
    if (mask_val == 0)
        return -EINVAL;
    uint32_t total_cores = count_set_bits(mask_val);
    if (total_cores <= 1)
    {
        SPDK_ERRLOG("Reactor mask must include at least 2 cores (1 for main, 1 for worker)\n");
        return -EINVAL;
    }
    g_num_worker_threads = total_cores - 1;

    if (init_async_ctx_pool() != 0)
        return -1;
    if (init_io_request_pool(g_num_worker_threads) != 0)
    {
        destroy_async_ctx_pool();
        return -1;
    }

    if (completion_loop)
    {
        g_completion_loop = completion_loop;
        Py_INCREF(g_completion_loop);
    }
    else
    {
        return -EINVAL;
    }

    spdk_thread_args_t *args = malloc(sizeof(spdk_thread_args_t));
    if (!args)
        return -ENOMEM;
    args->bdev_name = strdup(bdev_name);
    args->json_config_file = strdup(json_config_file);
    args->reactor_mask = strdup(reactor_mask);
    args->sock = strdup(sock);

    if (!args->bdev_name || !args->json_config_file || !args->reactor_mask || !args->sock)
    {
        free(args->bdev_name);
        free(args->json_config_file);
        free(args->reactor_mask);
        free(args->sock);
        free(args);
        return -ENOMEM;
    }

    if (pthread_create(&g_spdk_thread_id, NULL, spdk_thread_func, args) != 0)
    {
        free(args->bdev_name);
        free(args->json_config_file);
        free(args->reactor_mask);
        free(args->sock);
        free(args);
        return -1;
    }
    pthread_mutex_lock(&g_init_mutex);
    while (!g_spdk_ready && !g_spdk_init_failed)
        pthread_cond_wait(&g_init_cond, &g_init_mutex);
    pthread_mutex_unlock(&g_init_mutex);
    if (g_spdk_init_failed)
    {
        pthread_join(g_spdk_thread_id, NULL);
        g_spdk_thread_id = 0;
        spdk_app_fini();
        destroy_async_ctx_pool();
        destroy_io_request_pool();
        Py_DECREF(g_completion_loop);
        g_completion_loop = NULL;
        return -1;
    }
    DEBUG_LOG("SPDK is ready.");
    return 0;
}
int c_api_unload(void)
{
    if (g_spdk_thread_id == 0)
        return 0;
    if (g_main_spdk_thread)
        spdk_thread_send_msg(g_main_spdk_thread, _msg_shutdown, NULL);
    pthread_join(g_spdk_thread_id, NULL);

    destroy_async_ctx_pool();
    destroy_io_request_pool();

    if (g_completion_loop)
    {
        Py_DECREF(g_completion_loop);
        g_completion_loop = NULL;
    }

    spdk_app_fini();
    g_spdk_thread_id = 0;
    g_main_spdk_thread = NULL;
    g_spdk_ready = false;
    g_shutdown_initiated = false;
    g_spdk_init_failed = false;
    g_bs = NULL;
    g_unload_poller = NULL;
    free(g_worker_threads);
    g_worker_threads = NULL;
    return g_spdk_thread_rc;
}
io_request_context_t *c_api_alloc_io_request_ctx(int worker_id)
{
    if (worker_id < 0 || (uint32_t)worker_id >= g_num_worker_threads)
        return NULL;
    return alloc_io_request_ctx(&g_worker_threads[worker_id]);
}
void c_api_free_io_request_ctx(int worker_id, io_request_context_t *ctx)
{
    if (worker_id < 0 || (uint32_t)worker_id >= g_num_worker_threads || !ctx)
        return;
    free_io_request_ctx(ctx, &g_worker_threads[worker_id]);
}
int c_api_submit_batch_io_async(int worker_id, io_request_context_t **contexts, uint32_t count, PyObject *future)
{
    DEBUG_LOG("Submitting batch of %u IO requests to worker %d", count, worker_id);
    if (!g_spdk_ready || count == 0)
        return -1;
    if (worker_id < 0 || (uint32_t)worker_id >= g_num_worker_threads)
        return -EINVAL;

    worker_thread_t *worker = &g_worker_threads[worker_id];
    struct spdk_thread *target_thread = worker->thread;
    for (uint32_t i = 0; i < count; i++)
    {
        contexts[i]->worker = worker;
    }
    batch_io_context_t *batch = malloc(sizeof(batch_io_context_t));
    if (!batch)
        return -ENOMEM;
    batch->future = future;
    Py_INCREF(batch->future);
    atomic_init(&batch->ref_count, count);
    atomic_init(&batch->first_errno, 0);

    spdk_io_msg_t *msg = malloc(sizeof(spdk_io_msg_t));
    if (!msg)
    {
        free(batch);
        return -ENOMEM;
    }

    msg->contexts = malloc(count * sizeof(io_request_context_t *));
    if (!msg->contexts)
    {
        free(batch);
        free(msg);
        return -ENOMEM;
    }
    memcpy(msg->contexts, contexts, count * sizeof(io_request_context_t *));
    msg->count = count;
    msg->batch_ctx = batch;

    __sync_fetch_and_add(&g_async_ops_in_flight, 1);
    spdk_thread_send_msg(target_thread, _unified_enqueue_io_handler, msg);

    return 0;
}
int c_api_submit_single_io_async(int worker_id, io_request_context_t *ctx, PyObject *future)
{
    if (!g_spdk_ready || !ctx)
        return -1;
    if (worker_id < 0 || (uint32_t)worker_id >= g_num_worker_threads)
        return -EINVAL;

    worker_thread_t *worker = &g_worker_threads[worker_id];
    struct spdk_thread *target_thread = worker->thread;

    ctx->worker = worker;
    ctx->future = future;
    Py_INCREF(ctx->future);
    ctx->batch_ctx = NULL;

    spdk_io_msg_t *msg = malloc(sizeof(spdk_io_msg_t));
    if (!msg)
    {
        Py_DECREF(ctx->future);
        return -ENOMEM;
    }

    msg->contexts = malloc(1 * sizeof(io_request_context_t *));
    if (!msg->contexts)
    {
        Py_DECREF(ctx->future);
        free(msg);
        return -ENOMEM;
    }
    msg->contexts[0] = ctx;
    msg->count = 1;
    msg->batch_ctx = NULL;

    __sync_fetch_and_add(&g_async_ops_in_flight, 1);
    spdk_thread_send_msg(target_thread, _unified_enqueue_io_handler, msg);
    return 0;
}
int c_api_create_async(int num_clusters, PyObject *future)
{
    if (!g_spdk_ready)
        return -1;
    spdk_async_ctx *ctx = alloc_async_ctx();
    if (!ctx)
        return -ENOMEM;
    ctx->future = future;
    ctx->data.num_clusters = num_clusters;
    Py_INCREF(ctx->future);
    spdk_thread_send_msg(g_main_spdk_thread, _msg_create_blob, ctx);
    return 0;
}
int c_api_delete_async(spdk_blob_id blobid, PyObject *future)
{
    if (!g_spdk_ready)
        return -1;
    spdk_async_ctx *ctx = alloc_async_ctx();
    if (!ctx)
        return -ENOMEM;
    ctx->future = future;
    ctx->data.blobid = blobid;
    Py_INCREF(ctx->future);
    spdk_thread_send_msg(g_main_spdk_thread, _msg_delete_blob, ctx);
    return 0;
}
int c_api_open_blob_async(spdk_blob_id blobid, PyObject *future)
{
    if (!g_spdk_ready)
        return -1;
    spdk_async_ctx *ctx = alloc_async_ctx();
    if (!ctx)
        return -ENOMEM;
    ctx->future = future;
    ctx->data.blobid = blobid;
    Py_INCREF(ctx->future);
    spdk_thread_send_msg(g_main_spdk_thread, _msg_open_blob, ctx);
    return 0;
}
int c_api_close_blob_async(spdk_blob_handle handle, PyObject *future)
{
    if (!g_spdk_ready)
        return -1;
    spdk_async_ctx *ctx = alloc_async_ctx();
    if (!ctx)
        return -ENOMEM;
    ctx->future = future;
    ctx->data.handle = handle;
    Py_INCREF(ctx->future);
    spdk_thread_send_msg(g_main_spdk_thread, _msg_close_blob, ctx);
    return 0;
}
#define EXEC_SYNC_MSG(ctx_type, handler_func, cleanup_func, ...)      \
    do                                                                \
    {                                                                 \
        ctx_type ctx;                                                 \
        sem_init(&ctx.sem, 0, 0);                                     \
        __VA_ARGS__;                                                  \
        spdk_thread_send_msg(g_main_spdk_thread, handler_func, &ctx); \
        sem_wait(&ctx.sem);                                           \
        cleanup_func(&ctx.sem);                                       \
    } while (0)
void _sem_destroy_wrapper(sem_t *sem) { sem_destroy(sem); }
uint64_t c_api_get_page_size(void) { return g_cached_page_size; }
uint64_t c_api_get_cluster_size(void) { return g_cached_cluster_size; }
uint64_t c_api_get_free_clusters(void)
{
    if (!g_spdk_ready)
        return 0;
    uint64_t s = 0;
    EXEC_SYNC_MSG(msg_ctx_get_size, _msg_get_free_clusters, _sem_destroy_wrapper, ctx.size_out = &s);
    return s;
}
uint64_t c_api_get_io_unit_size(void) { return g_cached_io_unit_size; }
void *c_api_alloc_io_buffer(size_t size_bytes)
{
    if (!g_spdk_ready)
        return NULL;
    void *b = NULL;
    EXEC_SYNC_MSG(msg_ctx_buffer_op, _msg_alloc_buffer, _sem_destroy_wrapper, ctx.alloc.size_bytes = size_bytes; ctx.alloc.buffer_out = &b);
    return b;
}
void c_api_free_io_buffer(void *b)
{
    if (!b || !g_spdk_ready)
        return;
    EXEC_SYNC_MSG(msg_ctx_buffer_op, _msg_free_buffer, _sem_destroy_wrapper, ctx.free.buffer = b);
}

// --- 对象池实现 ---
static int init_io_request_pool(uint32_t num_workers)
{
    if (num_workers == 0)
        return -EINVAL;
    size_t total_contexts = IO_REQUEST_POOL_SIZE * num_workers;
    size_t pool_size = total_contexts * sizeof(io_request_context_t);
    g_io_request_pool.pool_buffer = aligned_alloc(CACHE_LINE_SIZE, pool_size);
    if (!g_io_request_pool.pool_buffer)
        return -ENOMEM;
    memset(g_io_request_pool.pool_buffer, 0, pool_size);
    return 0;
}
static void destroy_io_request_pool(void)
{
    if (g_io_request_pool.pool_buffer)
        free(g_io_request_pool.pool_buffer);
    g_io_request_pool.pool_buffer = NULL;
}
static io_request_context_t *alloc_io_request_ctx(worker_thread_t *worker)
{
    io_request_context_t *req_ctx = worker->io_req_freelist;
    if (req_ctx)
    {
        worker->io_req_freelist = (io_request_context_t *)req_ctx->link.tqe_next;
    }
    return req_ctx;
}
static void free_io_request_ctx(io_request_context_t *req_ctx, worker_thread_t *worker)
{
    if (!req_ctx)
        return;
    req_ctx->link.tqe_next = (struct io_request_context *)worker->io_req_freelist;
    worker->io_req_freelist = req_ctx;
}
static int init_async_ctx_pool(void)
{
    pthread_mutex_init(&g_async_ctx_pool.lock, NULL);
    g_async_ctx_pool.freelist = NULL;
    size_t pool_size = ASYNC_CTX_POOL_SIZE * sizeof(spdk_async_ctx);
    g_async_ctx_pool.pool_buffer = aligned_alloc(CACHE_LINE_SIZE, pool_size);
    if (!g_async_ctx_pool.pool_buffer)
        return -ENOMEM;
    memset(g_async_ctx_pool.pool_buffer, 0, pool_size);

    spdk_async_ctx *pool = (spdk_async_ctx *)g_async_ctx_pool.pool_buffer;
    for (int i = 0; i < ASYNC_CTX_POOL_SIZE; i++)
    {
        pool[i].next = g_async_ctx_pool.freelist;
        g_async_ctx_pool.freelist = &pool[i];
    }
    return 0;
}
static void destroy_async_ctx_pool(void)
{
    if (g_async_ctx_pool.pool_buffer)
        free(g_async_ctx_pool.pool_buffer);
    g_async_ctx_pool.pool_buffer = NULL;
    g_async_ctx_pool.freelist = NULL;
    pthread_mutex_destroy(&g_async_ctx_pool.lock);
}
static spdk_async_ctx *alloc_async_ctx(void)
{
    pthread_mutex_lock(&g_async_ctx_pool.lock);
    spdk_async_ctx *ctx = g_async_ctx_pool.freelist;
    if (ctx)
        g_async_ctx_pool.freelist = ctx->next;
    pthread_mutex_unlock(&g_async_ctx_pool.lock);
    return ctx;
}
static void free_async_ctx(spdk_async_ctx *ctx)
{
    if (!ctx)
        return;
    pthread_mutex_lock(&g_async_ctx_pool.lock);
    ctx->next = g_async_ctx_pool.freelist;
    g_async_ctx_pool.freelist = ctx;
    pthread_mutex_unlock(&g_async_ctx_pool.lock);
}