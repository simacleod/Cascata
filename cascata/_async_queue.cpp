#define PY_SSIZE_T_CLEAN
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <Python.h>
#include <structmember.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <string>

#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#ifdef __linux__
#include <linux/memfd.h>
#endif

namespace {

struct QueueControl {
    uint64_t capacity;
    uint64_t mask;
    uint64_t slot_size;
    uint64_t slot_stride;
    std::atomic<uint64_t> enqueue_pos;
    std::atomic<uint64_t> dequeue_pos;
    std::atomic<uint64_t> active_senders;
    std::atomic<uint64_t> close_flag;
};

struct alignas(64) SlotHeader {
    std::atomic<uint64_t> sequence;
    std::atomic<uint32_t> size;
    std::atomic<uint32_t> type;
};

enum SlotDataType : uint32_t {
    kSlotDataPickle = 0,
    kSlotDataInt64 = 1,
};

static PyObject* g_pickle_dumps = nullptr;
static PyObject* g_pickle_loads = nullptr;
static PyObject* g_pickle_highest_protocol = nullptr;

struct AsyncQueueObject {
    PyObject_HEAD
    int mem_fd;
    int items_fd;
    int space_fd;
    void* mem;
    size_t mem_size;
    QueueControl* ctrl;
    uint8_t* slots_base;
    bool using_shm_name;
    char shm_name[128];
};

constexpr uint64_t kMaxSlotSize = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max());
constexpr uint64_t kBroadcastNotify = static_cast<uint64_t>(std::numeric_limits<uint32_t>::max());

uint64_t next_power_of_two(uint64_t value) {
    if (value == 0) {
        return 1;
    }
    value--;
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value |= value >> 32;
    value++;
    return value;
}

SlotHeader* get_slot(AsyncQueueObject* self, uint64_t pos) {
    uint64_t index = pos & self->ctrl->mask;
    return reinterpret_cast<SlotHeader*>(self->slots_base + index * self->ctrl->slot_stride);
}

uint8_t* slot_data(SlotHeader* slot) {
    return reinterpret_cast<uint8_t*>(slot) + sizeof(SlotHeader);
}

bool notify_fd(int fd, uint64_t value = 1) {
    uint64_t to_write = value;
    while (true) {
        ssize_t written = ::write(fd, &to_write, sizeof(to_write));
        if (written == sizeof(to_write)) {
            return true;
        }
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN) {
                if (to_write > 1) {
                    to_write = 1;
                    continue;
                }
                return true;
            }
        }
        PyErr_SetFromErrno(PyExc_OSError);
        return false;
    }
}

bool set_cloexec(int fd) {
    int flags = ::fcntl(fd, F_GETFD);
    if (flags == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        return false;
    }
    if (::fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == -1) {
        PyErr_SetFromErrno(PyExc_OSError);
        return false;
    }
    return true;
}

int create_memfd(const char* name) {
#if defined(__linux__)
    int flags = MFD_CLOEXEC;
#ifdef MFD_ALLOW_SEALING
    flags |= MFD_ALLOW_SEALING;
#endif
#ifdef SYS_memfd_create
    int fd = static_cast<int>(::syscall(SYS_memfd_create, name, flags));
    if (fd >= 0) {
        return fd;
    }
#endif
#ifdef MFD_CLOEXEC
    int fd2 = ::memfd_create(name, flags);
    if (fd2 >= 0) {
        return fd2;
    }
#endif
#endif
    return -1;
}

int create_shared_file(char* out_name, size_t out_name_len, bool& using_shm) {
    int fd = create_memfd("cascata-channel");
    if (fd >= 0) {
        using_shm = false;
        if (!set_cloexec(fd)) {
            ::close(fd);
            return -1;
        }
        return fd;
    }

    using_shm = true;
    std::random_device rd;
    for (int attempt = 0; attempt < 128; ++attempt) {
        unsigned int value = rd();
        std::string name = "/cascata-" + std::to_string(::getpid()) + "-" + std::to_string(value);
        int shm_fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
        if (shm_fd >= 0) {
            if (!set_cloexec(shm_fd)) {
                ::close(shm_fd);
                ::shm_unlink(name.c_str());
                return -1;
            }
            std::strncpy(out_name, name.c_str(), out_name_len - 1);
            out_name[out_name_len - 1] = '\0';
            return shm_fd;
        }
        if (errno != EEXIST) {
            break;
        }
    }
    return -1;
}

bool map_memory(AsyncQueueObject* self) {
    self->mem = ::mmap(nullptr, self->mem_size, PROT_READ | PROT_WRITE, MAP_SHARED, self->mem_fd, 0);
    if (self->mem == MAP_FAILED) {
        self->mem = nullptr;
        PyErr_SetFromErrno(PyExc_OSError);
        return false;
    }
    self->ctrl = reinterpret_cast<QueueControl*>(self->mem);
    self->slots_base = reinterpret_cast<uint8_t*>(self->mem) + sizeof(QueueControl);
    return true;
}

void unmap_memory(AsyncQueueObject* self) {
    if (self->mem) {
        ::munmap(self->mem, self->mem_size);
        self->mem = nullptr;
    }
}

bool initialize_slots(AsyncQueueObject* self) {
    uint64_t capacity = self->ctrl->capacity;
    for (uint64_t i = 0; i < capacity; ++i) {
        SlotHeader* slot = get_slot(self, i);
        slot->sequence.store(i, std::memory_order_relaxed);
        slot->size.store(0, std::memory_order_relaxed);
        slot->type.store(kSlotDataPickle, std::memory_order_relaxed);
    }
    self->ctrl->enqueue_pos.store(0, std::memory_order_relaxed);
    self->ctrl->dequeue_pos.store(0, std::memory_order_relaxed);
    self->ctrl->active_senders.store(0, std::memory_order_relaxed);
    self->ctrl->close_flag.store(0, std::memory_order_relaxed);
    return true;
}

int queue_try_push_bytes(AsyncQueueObject* self, const void* data, uint32_t size, uint32_t type) {
    if (size > self->ctrl->slot_size) {
        PyErr_SetString(PyExc_ValueError, "payload exceeds slot size");
        return -1;
    }

    if (self->ctrl->close_flag.load(std::memory_order_acquire)) {
        return 1;
    }

    while (true) {
        uint64_t pos = self->ctrl->enqueue_pos.load(std::memory_order_relaxed);
        SlotHeader* slot = get_slot(self, pos);
        uint64_t seq = slot->sequence.load(std::memory_order_acquire);
        intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
        if (diff == 0) {
            if (self->ctrl->enqueue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                if (size > 0) {
                    std::memcpy(slot_data(slot), data, static_cast<size_t>(size));
                }
                slot->size.store(size, std::memory_order_relaxed);
                slot->type.store(type, std::memory_order_relaxed);
                slot->sequence.store(pos + 1, std::memory_order_release);
                if (!notify_fd(self->items_fd)) {
                    return -1;
                }
                return 0;
            }
        } else if (diff < 0) {
            return 2;
        } else {
            continue;
        }
    }
}

bool queue_release_slot(AsyncQueueObject* self, SlotHeader* slot, uint64_t pos) {
    slot->sequence.store(pos + self->ctrl->capacity, std::memory_order_release);
    return notify_fd(self->space_fd);
}

}  // namespace

static void AsyncQueue_dealloc(PyObject* self_obj) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    unmap_memory(self);
    if (self->mem_fd >= 0) {
        ::close(self->mem_fd);
        self->mem_fd = -1;
    }
    if (self->items_fd >= 0) {
        ::close(self->items_fd);
        self->items_fd = -1;
    }
    if (self->space_fd >= 0) {
        ::close(self->space_fd);
        self->space_fd = -1;
    }
    if (self->using_shm_name && self->shm_name[0] != '\0') {
        ::shm_unlink(self->shm_name);
        self->shm_name[0] = '\0';
    }
    Py_TYPE(self)->tp_free(self_obj);
}

static int AsyncQueue_init(PyObject* self_obj, PyObject* args, PyObject* kwds) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    unsigned long long capacity_arg;
    unsigned long long slot_size_arg = 65536ULL;
    static const char* keywords[] = {"capacity", "slot_size", nullptr};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "K|K", const_cast<char**>(keywords), &capacity_arg, &slot_size_arg)) {
        return -1;
    }
    if (capacity_arg == 0) {
        PyErr_SetString(PyExc_ValueError, "capacity must be greater than zero");
        return -1;
    }
    if (slot_size_arg == 0 || slot_size_arg > kMaxSlotSize) {
        PyErr_SetString(PyExc_ValueError, "slot_size must be between 1 and 2**32-1");
        return -1;
    }

    std::memset(self->shm_name, 0, sizeof(self->shm_name));
    self->mem_fd = -1;
    self->items_fd = -1;
    self->space_fd = -1;
    self->mem = nullptr;
    self->ctrl = nullptr;
    self->slots_base = nullptr;
    self->mem_size = 0;
    self->using_shm_name = false;

    bool using_shm = false;
    int mem_fd = create_shared_file(self->shm_name, sizeof(self->shm_name), using_shm);
    if (mem_fd < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    self->mem_fd = mem_fd;
    self->using_shm_name = using_shm;

    uint64_t capacity = next_power_of_two(capacity_arg);
    uint64_t slot_size = slot_size_arg;
    uint64_t stride = sizeof(SlotHeader) + slot_size;
    const uint64_t alignment = 64;
    if (stride % alignment != 0) {
        stride += alignment - (stride % alignment);
    }

    uint64_t control_size = sizeof(QueueControl);
    uint64_t slots_size = capacity * stride;
    uint64_t total_size = control_size + slots_size;

    if (::ftruncate(self->mem_fd, static_cast<off_t>(total_size)) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    self->mem_size = static_cast<size_t>(total_size);
    if (!map_memory(self)) {
        return -1;
    }

    self->ctrl->capacity = capacity;
    self->ctrl->mask = capacity - 1;
    self->ctrl->slot_size = slot_size;
    self->ctrl->slot_stride = stride;
    initialize_slots(self);

    int event_flags = EFD_NONBLOCK | EFD_CLOEXEC | EFD_SEMAPHORE;
    self->items_fd = ::eventfd(0, event_flags);
    if (self->items_fd < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    self->space_fd = ::eventfd(0, event_flags);
    if (self->space_fd < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }

    return 0;
}

static PyObject* AsyncQueue_try_push(PyObject* self_obj, PyObject* arg) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    Py_buffer buffer;
    if (PyObject_GetBuffer(arg, &buffer, PyBUF_SIMPLE) < 0) {
        return nullptr;
    }
    int status = queue_try_push_bytes(self, buffer.buf, static_cast<uint32_t>(buffer.len), kSlotDataPickle);
    PyBuffer_Release(&buffer);
    if (status < 0) {
        return nullptr;
    }
    return PyLong_FromLong(status);
}

static PyObject* AsyncQueue_try_pop(PyObject* self_obj, PyObject* Py_UNUSED(arg)) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    while (true) {
        uint64_t pos = self->ctrl->dequeue_pos.load(std::memory_order_relaxed);
        SlotHeader* slot = get_slot(self, pos);
        uint64_t seq = slot->sequence.load(std::memory_order_acquire);
        intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
        if (diff == 0) {
            if (self->ctrl->dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                uint32_t size = slot->size.load(std::memory_order_relaxed);
                PyObject* data = PyBytes_FromStringAndSize(reinterpret_cast<char*>(slot_data(slot)), size);
                if (!data) {
                    return nullptr;
                }
                if (!queue_release_slot(self, slot, pos)) {
                    Py_DECREF(data);
                    return nullptr;
                }
                PyObject* result = PyTuple_New(2);
                if (!result) {
                    Py_DECREF(data);
                    return nullptr;
                }
                PyTuple_SET_ITEM(result, 0, data);
                PyObject* status = PyLong_FromLong(0);
                if (!status) {
                    Py_DECREF(result);
                    return nullptr;
                }
                PyTuple_SET_ITEM(result, 1, status);
                return result;
            }
        } else if (diff < 0) {
            bool closed = self->ctrl->close_flag.load(std::memory_order_acquire) != 0;
            PyObject* result = PyTuple_New(2);
            if (!result) {
                return nullptr;
            }
            Py_INCREF(Py_None);
            PyTuple_SET_ITEM(result, 0, Py_None);
            PyObject* status = PyLong_FromLong(closed ? 2 : 1);
            if (!status) {
                Py_DECREF(result);
                return nullptr;
            }
            PyTuple_SET_ITEM(result, 1, status);
            return result;
        } else {
            continue;
        }
    }
}

static PyObject* AsyncQueue_try_push_object(PyObject* self_obj, PyObject* arg) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    if (!g_pickle_dumps || !g_pickle_highest_protocol) {
        PyErr_SetString(PyExc_RuntimeError, "pickle helpers not initialized");
        return nullptr;
    }

    if (PyLong_CheckExact(arg)) {
        int overflow = 0;
        long long value = PyLong_AsLongLongAndOverflow(arg, &overflow);
        if (overflow == 0) {
            uint8_t buffer[sizeof(long long)];
            std::memcpy(buffer, &value, sizeof(buffer));
            int status = queue_try_push_bytes(self, buffer, static_cast<uint32_t>(sizeof(buffer)), kSlotDataInt64);
            if (status < 0) {
                return nullptr;
            }
            return PyLong_FromLong(status);
        }
    }

    PyObject* payload = PyObject_CallFunctionObjArgs(g_pickle_dumps, arg, g_pickle_highest_protocol, nullptr);
    if (!payload) {
        return nullptr;
    }

    Py_buffer buffer;
    if (PyObject_GetBuffer(payload, &buffer, PyBUF_SIMPLE) < 0) {
        Py_DECREF(payload);
        return nullptr;
    }
    int status = queue_try_push_bytes(self, buffer.buf, static_cast<uint32_t>(buffer.len), kSlotDataPickle);
    PyBuffer_Release(&buffer);
    Py_DECREF(payload);
    if (status < 0) {
        return nullptr;
    }
    return PyLong_FromLong(status);
}

static PyObject* AsyncQueue_try_pop_object(PyObject* self_obj, PyObject* Py_UNUSED(arg)) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    if (!g_pickle_loads) {
        PyErr_SetString(PyExc_RuntimeError, "pickle helpers not initialized");
        return nullptr;
    }
    while (true) {
        uint64_t pos = self->ctrl->dequeue_pos.load(std::memory_order_relaxed);
        SlotHeader* slot = get_slot(self, pos);
        uint64_t seq = slot->sequence.load(std::memory_order_acquire);
        intptr_t diff = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);
        if (diff == 0) {
            if (self->ctrl->dequeue_pos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
                uint32_t size = slot->size.load(std::memory_order_relaxed);
                uint32_t type = slot->type.load(std::memory_order_relaxed);
                PyObject* item = nullptr;
                if (type == kSlotDataInt64 && size == sizeof(long long)) {
                    long long value;
                    std::memcpy(&value, slot_data(slot), sizeof(value));
                    item = PyLong_FromLongLong(value);
                } else {
                    PyObject* view = PyMemoryView_FromMemory(reinterpret_cast<char*>(slot_data(slot)), size, PyBUF_READ);
                    if (!view) {
                        queue_release_slot(self, slot, pos);
                        return nullptr;
                    }
                    item = PyObject_CallFunctionObjArgs(g_pickle_loads, view, nullptr);
                    Py_DECREF(view);
                }
                if (!queue_release_slot(self, slot, pos)) {
                    Py_XDECREF(item);
                    return nullptr;
                }
                if (!item) {
                    return nullptr;
                }
                PyObject* result = PyTuple_New(2);
                if (!result) {
                    Py_DECREF(item);
                    return nullptr;
                }
                PyTuple_SET_ITEM(result, 0, item);
                PyObject* status = PyLong_FromLong(0);
                if (!status) {
                    Py_DECREF(result);
                    return nullptr;
                }
                PyTuple_SET_ITEM(result, 1, status);
                return result;
            }
        } else if (diff < 0) {
            bool closed = self->ctrl->close_flag.load(std::memory_order_acquire) != 0;
            PyObject* result = PyTuple_New(2);
            if (!result) {
                return nullptr;
            }
            Py_INCREF(Py_None);
            PyTuple_SET_ITEM(result, 0, Py_None);
            PyObject* status = PyLong_FromLong(closed ? 2 : 1);
            if (!status) {
                Py_DECREF(result);
                return nullptr;
            }
            PyTuple_SET_ITEM(result, 1, status);
            return result;
        } else {
            continue;
        }
    }
}

static PyObject* AsyncQueue_add_sender(PyObject* self_obj, PyObject* Py_UNUSED(arg)) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    self->ctrl->close_flag.store(0, std::memory_order_release);
    uint64_t count = self->ctrl->active_senders.fetch_add(1, std::memory_order_acq_rel) + 1;
    return PyLong_FromUnsignedLongLong(count);
}

static PyObject* AsyncQueue_release_sender(PyObject* self_obj, PyObject* Py_UNUSED(arg)) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    uint64_t previous = self->ctrl->active_senders.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 0) {
        self->ctrl->active_senders.fetch_add(1, std::memory_order_relaxed);
        PyErr_SetString(PyExc_RuntimeError, "release_sender called more times than add_sender");
        return nullptr;
    }
    uint64_t remaining = previous - 1;
    if (remaining == 0) {
        self->ctrl->close_flag.store(1, std::memory_order_release);
        if (!notify_fd(self->items_fd, kBroadcastNotify)) {
            return nullptr;
        }
        if (!notify_fd(self->space_fd, kBroadcastNotify)) {
            return nullptr;
        }
    }
    return PyLong_FromUnsignedLongLong(remaining);
}

static PyObject* AsyncQueue_is_closed(PyObject* self_obj, PyObject* Py_UNUSED(arg)) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    bool closed = self->ctrl->close_flag.load(std::memory_order_acquire) != 0;
    if (closed) {
        Py_RETURN_TRUE;
    }
    Py_RETURN_FALSE;
}

static PyObject* AsyncQueue_active_senders(PyObject* self_obj, PyObject* Py_UNUSED(arg)) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    uint64_t value = self->ctrl->active_senders.load(std::memory_order_acquire);
    return PyLong_FromUnsignedLongLong(value);
}

static PyObject* AsyncQueue_filenos(PyObject* self_obj, PyObject* Py_UNUSED(arg)) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    PyObject* tuple = PyTuple_New(2);
    if (!tuple) {
        return nullptr;
    }
    PyObject* items = PyLong_FromLong(self->items_fd);
    if (!items) {
        Py_DECREF(tuple);
        return nullptr;
    }
    PyObject* space = PyLong_FromLong(self->space_fd);
    if (!space) {
        Py_DECREF(items);
        Py_DECREF(tuple);
        return nullptr;
    }
    PyTuple_SET_ITEM(tuple, 0, items);
    PyTuple_SET_ITEM(tuple, 1, space);
    return tuple;
}

static PyObject* AsyncQueue_share(PyObject* self_obj, PyObject* Py_UNUSED(arg)) {
    auto* self = reinterpret_cast<AsyncQueueObject*>(self_obj);
    PyObject* tuple = PyTuple_New(4);
    if (!tuple) {
        return nullptr;
    }
    PyObject* mem_fd = PyLong_FromLong(self->mem_fd);
    if (!mem_fd) {
        Py_DECREF(tuple);
        return nullptr;
    }
    PyObject* mem_size = PyLong_FromUnsignedLongLong(self->mem_size);
    if (!mem_size) {
        Py_DECREF(mem_fd);
        Py_DECREF(tuple);
        return nullptr;
    }
    PyObject* items_fd = PyLong_FromLong(self->items_fd);
    if (!items_fd) {
        Py_DECREF(mem_fd);
        Py_DECREF(mem_size);
        Py_DECREF(tuple);
        return nullptr;
    }
    PyObject* space_fd = PyLong_FromLong(self->space_fd);
    if (!space_fd) {
        Py_DECREF(mem_fd);
        Py_DECREF(mem_size);
        Py_DECREF(items_fd);
        Py_DECREF(tuple);
        return nullptr;
    }
    PyTuple_SET_ITEM(tuple, 0, mem_fd);
    PyTuple_SET_ITEM(tuple, 1, mem_size);
    PyTuple_SET_ITEM(tuple, 2, items_fd);
    PyTuple_SET_ITEM(tuple, 3, space_fd);
    return tuple;
}

static PyObject* AsyncQueue_from_handles(PyTypeObject* type, PyObject* args) {
    int mem_fd;
    unsigned long long mem_size;
    int items_fd;
    int space_fd;
    if (!PyArg_ParseTuple(args, "iKii", &mem_fd, &mem_size, &items_fd, &space_fd)) {
        return nullptr;
    }
    auto* self = reinterpret_cast<AsyncQueueObject*>(type->tp_alloc(type, 0));
    if (!self) {
        return nullptr;
    }
    std::memset(self->shm_name, 0, sizeof(self->shm_name));
    self->using_shm_name = false;
    self->mem_fd = mem_fd;
    self->items_fd = items_fd;
    self->space_fd = space_fd;
    self->mem_size = static_cast<size_t>(mem_size);
    self->mem = nullptr;
    self->ctrl = nullptr;
    self->slots_base = nullptr;
    if (!set_cloexec(self->mem_fd) || !set_cloexec(self->items_fd) || !set_cloexec(self->space_fd)) {
        Py_DECREF(self);
        return nullptr;
    }
    if (!map_memory(self)) {
        Py_DECREF(self);
        return nullptr;
    }
    return reinterpret_cast<PyObject*>(self);
}

static PyMethodDef AsyncQueue_methods[] = {
    {"try_push", reinterpret_cast<PyCFunction>(AsyncQueue_try_push), METH_O, PyDoc_STR("try_push(data) -> status")},
    {"try_pop", reinterpret_cast<PyCFunction>(AsyncQueue_try_pop), METH_NOARGS, PyDoc_STR("try_pop() -> (data, status)")},
    {"try_push_object", reinterpret_cast<PyCFunction>(AsyncQueue_try_push_object), METH_O, PyDoc_STR("try_push_object(obj) -> status")},
    {"try_pop_object", reinterpret_cast<PyCFunction>(AsyncQueue_try_pop_object), METH_NOARGS, PyDoc_STR("try_pop_object() -> (object, status)")},
    {"add_sender", reinterpret_cast<PyCFunction>(AsyncQueue_add_sender), METH_NOARGS, PyDoc_STR("increment sender count")},
    {"release_sender", reinterpret_cast<PyCFunction>(AsyncQueue_release_sender), METH_NOARGS, PyDoc_STR("decrement sender count")},
    {"is_closed", reinterpret_cast<PyCFunction>(AsyncQueue_is_closed), METH_NOARGS, PyDoc_STR("return True if queue closed")},
    {"active_senders", reinterpret_cast<PyCFunction>(AsyncQueue_active_senders), METH_NOARGS, PyDoc_STR("return active sender count")},
    {"filenos", reinterpret_cast<PyCFunction>(AsyncQueue_filenos), METH_NOARGS, PyDoc_STR("return item and space eventfds")},
    {"share", reinterpret_cast<PyCFunction>(AsyncQueue_share), METH_NOARGS, PyDoc_STR("return handles for pickling")},
    {"from_handles", reinterpret_cast<PyCFunction>(AsyncQueue_from_handles), METH_CLASS | METH_VARARGS, PyDoc_STR("create queue from fds")},
    {nullptr, nullptr, 0, nullptr}
};

static PyTypeObject AsyncQueueType = {
    PyVarObject_HEAD_INIT(nullptr, 0)
    .tp_name = "_async_queue.AsyncQueue",
    .tp_basicsize = sizeof(AsyncQueueObject),
    .tp_dealloc = AsyncQueue_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = PyDoc_STR("Lock-free multi-producer multi-consumer queue"),
    .tp_methods = AsyncQueue_methods,
    .tp_init = AsyncQueue_init,
    .tp_new = PyType_GenericNew,
};

static PyMethodDef module_methods[] = {
    {nullptr, nullptr, 0, nullptr}
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_async_queue",
    .m_doc = "C++ bindings for Cascata channel queue",
    .m_size = -1,
    .m_methods = module_methods,
};

PyMODINIT_FUNC PyInit__async_queue(void) {
    if (PyType_Ready(&AsyncQueueType) < 0) {
        return nullptr;
    }
    PyObject* module = PyModule_Create(&moduledef);
    if (!module) {
        return nullptr;
    }
    Py_INCREF(&AsyncQueueType);
    if (PyModule_AddObject(module, "AsyncQueue", reinterpret_cast<PyObject*>(&AsyncQueueType)) < 0) {
        Py_DECREF(&AsyncQueueType);
        Py_DECREF(module);
        return nullptr;
    }
    PyObject* pickle_module = PyImport_ImportModule("pickle");
    if (!pickle_module) {
        Py_DECREF(module);
        return nullptr;
    }
    g_pickle_dumps = PyObject_GetAttrString(pickle_module, "dumps");
    g_pickle_loads = PyObject_GetAttrString(pickle_module, "loads");
    g_pickle_highest_protocol = PyObject_GetAttrString(pickle_module, "HIGHEST_PROTOCOL");
    Py_DECREF(pickle_module);
    if (!g_pickle_dumps || !g_pickle_loads || !g_pickle_highest_protocol) {
        Py_XDECREF(g_pickle_dumps);
        Py_XDECREF(g_pickle_loads);
        Py_XDECREF(g_pickle_highest_protocol);
        g_pickle_dumps = nullptr;
        g_pickle_loads = nullptr;
        g_pickle_highest_protocol = nullptr;
        Py_DECREF(module);
        return nullptr;
    }
    return module;
}
