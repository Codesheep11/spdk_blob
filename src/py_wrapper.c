#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include "spdk_wrapper.h"

static PyObject *retcode_to_py(int rc)
{
    if (rc == 0)
    {
        Py_RETURN_NONE;
    }
    PyErr_Format(PyExc_OSError, "SPDK C-API call failed with error code: %d", rc);
    return NULL;
}

static PyObject *py_c_api_init(PyObject *self, PyObject *args)
{
    const char *bdev, *json, *mask, *sock;
    PyObject *completion_loop;
    if (!PyArg_ParseTuple(args, "ssssO:init", &bdev, &json, &mask, &sock, &completion_loop))
        return NULL;
    return retcode_to_py(c_api_init(bdev, json, mask, sock, completion_loop));
}

static PyObject *py_c_api_unload(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    return retcode_to_py(c_api_unload());
}

static PyObject *py_c_api_register_io_thread(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    int worker_id = c_api_register_io_thread();
    if (worker_id < 0)
    {
        PyErr_SetString(PyExc_RuntimeError, "Failed to register IO thread and bind SPDK worker.");
        return NULL;
    }
    return PyLong_FromLong(worker_id);
}

static PyObject *py_c_api_unregister_io_thread(PyObject *self, PyObject *Py_UNUSED(ignored))
{
    c_api_unregister_io_thread();
    Py_RETURN_NONE;
}

static PyObject *py_c_api_create_async(PyObject *self, PyObject *args)
{
    int num_clusters;
    PyObject *future;
    if (!PyArg_ParseTuple(args, "iO:create_async", &num_clusters, &future))
        return NULL;
    c_api_create_async(num_clusters, future);
    Py_RETURN_NONE;
}
static PyObject *py_c_api_delete_async(PyObject *self, PyObject *args)
{
    unsigned long long id;
    PyObject *future;
    if (!PyArg_ParseTuple(args, "KO:delete_async", &id, &future))
        return NULL;
    c_api_delete_async((spdk_blob_id)id, future);
    Py_RETURN_NONE;
}
static PyObject *py_c_api_open_async(PyObject *self, PyObject *args)
{
    unsigned long long id;
    PyObject *future;
    if (!PyArg_ParseTuple(args, "KO:open_async", &id, &future))
        return NULL;
    c_api_open_blob_async((spdk_blob_id)id, future);
    Py_RETURN_NONE;
}
static PyObject *py_c_api_close_async(PyObject *self, PyObject *args)
{
    PyObject *handle_obj, *future;
    if (!PyArg_ParseTuple(args, "OO:close_async", &handle_obj, &future))
        return NULL;
    spdk_blob_handle handle = (spdk_blob_handle)PyLong_AsVoidPtr(handle_obj);
    if (PyErr_Occurred())
        return NULL;
    c_api_close_blob_async(handle, future);
    Py_RETURN_NONE;
}

static PyObject *py_c_api_write_async(PyObject *self, PyObject *args)
{
    PyObject *handle_obj, *payload_obj, *future;
    unsigned long long offset_io_units, num_io_units;
    int worker_id;
    if (!PyArg_ParseTuple(args, "iOOKKO:write_async", &worker_id, &handle_obj, &payload_obj, &offset_io_units, &num_io_units, &future))
        return NULL;

    io_request_context_t *ctx = c_api_alloc_io_request_ctx(worker_id);
    if (!ctx)
        return PyErr_NoMemory();

    ctx->handle = (spdk_blob_handle)PyLong_AsVoidPtr(handle_obj);
    ctx->payload = PyLong_AsVoidPtr(payload_obj);
    if (PyErr_Occurred())
    {
        c_api_free_io_request_ctx(worker_id, ctx);
        return NULL;
    }

    ctx->offset_io_units = offset_io_units;
    ctx->num_io_units = num_io_units;
    ctx->is_read = false;

    c_api_submit_single_io_async(worker_id, ctx, future);

    Py_RETURN_NONE;
}

static PyObject *py_c_api_read_async(PyObject *self, PyObject *args)
{
    PyObject *handle_obj, *payload_obj, *future;
    unsigned long long offset_io_units, num_io_units;
    int worker_id;
    if (!PyArg_ParseTuple(args, "iOOKKO:read_async", &worker_id, &handle_obj, &payload_obj, &offset_io_units, &num_io_units, &future))
        return NULL;

    io_request_context_t *ctx = c_api_alloc_io_request_ctx(worker_id);
    if (!ctx)
        return PyErr_NoMemory();

    ctx->handle = (spdk_blob_handle)PyLong_AsVoidPtr(handle_obj);
    ctx->payload = PyLong_AsVoidPtr(payload_obj);
    if (PyErr_Occurred())
    {
        c_api_free_io_request_ctx(worker_id, ctx);
        return NULL;
    }

    ctx->offset_io_units = offset_io_units;
    ctx->num_io_units = num_io_units;
    ctx->is_read = true;

    c_api_submit_single_io_async(worker_id, ctx, future);

    Py_RETURN_NONE;
}

static PyObject *py_c_api_write_batch_async(PyObject *self, PyObject *args)
{
    int worker_id;
    PyObject *requests_list, *future;
    if (!PyArg_ParseTuple(args, "iO!O:write_batch_async", &worker_id, &PyList_Type, &requests_list, &future))
        return NULL;

    Py_ssize_t count = PyList_GET_SIZE(requests_list);
    if (count == 0)
        Py_RETURN_NONE;

    io_request_context_t **c_contexts = malloc(count * sizeof(io_request_context_t *));
    if (!c_contexts)
        return PyErr_NoMemory();

    Py_ssize_t allocated_count = 0;
    bool error_occurred = false;

    for (Py_ssize_t i = 0; i < count; i++)
    {
        PyObject *item = PyList_GET_ITEM(requests_list, i);
        PyObject *handle_obj, *payload_obj;
        unsigned long long offset, num;

        if (!PyArg_ParseTuple(item, "OOKK", &handle_obj, &payload_obj, &offset, &num))
        {
            PyErr_SetString(PyExc_TypeError, "Each request in the batch must be a tuple of (handle, payload_ptr, offset_units, num_units)");
            error_occurred = true;
            break;
        }

        io_request_context_t *ctx = c_api_alloc_io_request_ctx(worker_id);
        if (!ctx)
        {
            PyErr_SetString(PyExc_MemoryError, "Failed to allocate io_request_context from pool");
            error_occurred = true;
            break;
        }
        c_contexts[allocated_count++] = ctx;

        ctx->handle = (spdk_blob_handle)PyLong_AsVoidPtr(handle_obj);
        ctx->payload = PyLong_AsVoidPtr(payload_obj);
        ctx->offset_io_units = offset;
        ctx->num_io_units = num;
        ctx->is_read = false;

        if (PyErr_Occurred())
        {
            error_occurred = true;
            break;
        }
    }

    if (error_occurred)
    {
        for (Py_ssize_t i = 0; i < allocated_count; i++)
        {
            c_api_free_io_request_ctx(worker_id, c_contexts[i]);
        }
        free(c_contexts);
        return NULL;
    }

    c_api_submit_batch_io_async(worker_id, c_contexts, (uint32_t)count, future);

    free(c_contexts);
    Py_RETURN_NONE;
}

static PyObject *py_c_api_read_batch_async(PyObject *self, PyObject *args)
{
    int worker_id;
    PyObject *requests_list, *future;
    if (!PyArg_ParseTuple(args, "iO!O:read_batch_async", &worker_id, &PyList_Type, &requests_list, &future))
        return NULL;

    Py_ssize_t count = PyList_GET_SIZE(requests_list);
    if (count == 0)
        Py_RETURN_NONE;

    io_request_context_t **c_contexts = malloc(count * sizeof(io_request_context_t *));
    if (!c_contexts)
        return PyErr_NoMemory();

    Py_ssize_t allocated_count = 0;
    bool error_occurred = false;

    for (Py_ssize_t i = 0; i < count; i++)
    {
        PyObject *item = PyList_GET_ITEM(requests_list, i);
        PyObject *handle_obj, *payload_obj;
        unsigned long long offset, num;

        if (!PyArg_ParseTuple(item, "OOKK", &handle_obj, &payload_obj, &offset, &num))
        {
            PyErr_SetString(PyExc_TypeError, "Each request in the batch must be a tuple of (handle, payload_ptr, offset_units, num_units)");
            error_occurred = true;
            break;
        }

        io_request_context_t *ctx = c_api_alloc_io_request_ctx(worker_id);
        if (!ctx)
        {
            PyErr_SetString(PyExc_MemoryError, "Failed to allocate io_request_context from pool");
            error_occurred = true;
            break;
        }
        c_contexts[allocated_count++] = ctx;

        ctx->handle = (spdk_blob_handle)PyLong_AsVoidPtr(handle_obj);
        ctx->payload = PyLong_AsVoidPtr(payload_obj);
        ctx->offset_io_units = offset;
        ctx->num_io_units = num;
        ctx->is_read = true; // For read batch

        if (PyErr_Occurred())
        {
            error_occurred = true;
            break;
        }
    }

    if (error_occurred)
    {
        for (Py_ssize_t i = 0; i < allocated_count; i++)
        {
            c_api_free_io_request_ctx(worker_id, c_contexts[i]);
        }
        free(c_contexts);
        return NULL;
    }

    c_api_submit_batch_io_async(worker_id, c_contexts, (uint32_t)count, future);

    free(c_contexts);
    Py_RETURN_NONE;
}

static PyObject *py_c_api_get_page_size(PyObject *self, PyObject *Py_UNUSED(ignored)) { return PyLong_FromUnsignedLongLong(c_api_get_page_size()); }
static PyObject *py_c_api_get_cluster_size(PyObject *self, PyObject *Py_UNUSED(ignored)) { return PyLong_FromUnsignedLongLong(c_api_get_cluster_size()); }
static PyObject *py_c_api_get_free_cluster_count(PyObject *self, PyObject *Py_UNUSED(ignored)) { return PyLong_FromUnsignedLongLong(c_api_get_free_clusters()); }
static PyObject *py_c_api_get_io_unit_size(PyObject *self, PyObject *Py_UNUSED(ignored)) { return PyLong_FromUnsignedLongLong(c_api_get_io_unit_size()); }
static PyObject *py_c_api_alloc_io_buffer(PyObject *self, PyObject *args)
{
    unsigned long long s;
    if (!PyArg_ParseTuple(args, "K:alloc_io_buffer", &s))
        return NULL;
    void *b = c_api_alloc_io_buffer((size_t)s);
    if (b == NULL)
    {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate SPDK IO buffer");
        return NULL;
    }
    return PyLong_FromVoidPtr(b);
}
static PyObject *py_c_api_alloc_io_buffer_view(PyObject *self, PyObject *args)
{
    unsigned long long s;
    if (!PyArg_ParseTuple(args, "K:alloc_io_buffer_view", &s))
        return NULL;
    void *b = c_api_alloc_io_buffer((size_t)s);
    if (b == NULL)
    {
        PyErr_SetString(PyExc_MemoryError, "Failed to allocate SPDK IO buffer");
        return NULL;
    }
    PyObject *m = PyMemoryView_FromMemory(b, (Py_ssize_t)s, PyBUF_WRITE);
    if (m == NULL)
    {
        c_api_free_io_buffer(b);
        return PyErr_NoMemory();
    }
    PyObject *p = PyLong_FromVoidPtr(b);
    if (p == NULL)
    {
        Py_DECREF(m);
        c_api_free_io_buffer(b);
        return PyErr_NoMemory();
    }
    return PyTuple_Pack(2, m, p);
}
static PyObject *py_c_api_free_io_buffer(PyObject *self, PyObject *args)
{
    PyObject *buffer_obj;
    if (!PyArg_ParseTuple(args, "O:free_io_buffer", &buffer_obj))
        return NULL;
    void *b = PyLong_AsVoidPtr(buffer_obj);
    if (PyErr_Occurred())
        return NULL;
    c_api_free_io_buffer(b);
    Py_RETURN_NONE;
}

PyMODINIT_FUNC PyInit_spdk_blob(void);

static PyMethodDef SpdkMethods[] = {
    {"init", py_c_api_init, METH_VARARGS, "Initialize the SPDK environment, optionally with an RPC socket."},
    {"unload", py_c_api_unload, METH_NOARGS, "Shut down the SPDK environment."},
    {"register_io_thread", py_c_api_register_io_thread, METH_NOARGS, "Register the calling thread for I/O and get a dedicated worker ID."},
    {"unregister_io_thread", py_c_api_unregister_io_thread, METH_NOARGS, "Unregister the calling thread."},
    {"create_async", py_c_api_create_async, METH_VARARGS, "Asynchronously create a blob."},
    {"delete_async", py_c_api_delete_async, METH_VARARGS, "Asynchronously delete a blob."},
    {"open_async", py_c_api_open_async, METH_VARARGS, "Asynchronously open a blob."},
    {"close_async", py_c_api_close_async, METH_VARARGS, "Asynchronously close a blob."},
    {"write_async", py_c_api_write_async, METH_VARARGS, "Asynchronously write to a blob on a specific worker."},
    {"read_async", py_c_api_read_async, METH_VARARGS, "Asynchronously read from a blob on a specific worker."},
    {"write_batch_async", py_c_api_write_batch_async, METH_VARARGS, "Asynchronously write a batch of requests to blobs."},
    {"read_batch_async", py_c_api_read_batch_async, METH_VARARGS, "Asynchronously read a batch of requests from blobs."},
    {"get_page_size", py_c_api_get_page_size, METH_NOARGS, "Get page size."},
    {"get_cluster_size", py_c_api_get_cluster_size, METH_NOARGS, "Get cluster size."},
    {"get_free_cluster_count", py_c_api_get_free_cluster_count, METH_NOARGS, "Get free cluster count."},
    {"get_io_unit_size", py_c_api_get_io_unit_size, METH_NOARGS, "Get I/O unit size."},
    {"alloc_io_buffer", py_c_api_alloc_io_buffer, METH_VARARGS, "Allocate a DMA-safe IO buffer."},
    {"alloc_io_buffer_view", py_c_api_alloc_io_buffer_view, METH_VARARGS, "Allocate a DMA-safe IO buffer as a memoryview."},
    {"free_io_buffer", py_c_api_free_io_buffer, METH_VARARGS, "Free a DMA-safe IO buffer."},
    {NULL, NULL, 0, NULL}};
static struct PyModuleDef spdkmodule = {
    PyModuleDef_HEAD_INIT,
    "spdk_blob",
    "Python binding for a custom SPDK C API",
    -1,
    SpdkMethods,
};
PyMODINIT_FUNC PyInit_spdk_blob(void) { return PyModule_Create(&spdkmodule); }