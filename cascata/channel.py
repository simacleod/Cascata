import asyncio
import os
from collections import deque
from contextlib import asynccontextmanager
from multiprocessing import reduction
from typing import Any, Deque, Dict
from ._async_queue import AsyncQueue


def _detach_handle(handle: Any) -> int:
    detach = getattr(handle, "detach", None)
    if callable(detach):
        return int(detach())
    return int(handle)


class ChannelClosed(Exception):
    """Raised when attempting to send to a closed channel."""


class _FDWaitQueue:
    __slots__ = ("_fd", "_waiters", "_loop", "_reader_active")

    def __init__(self, fd: int) -> None:
        self._fd = fd
        self._waiters: Deque[asyncio.Future[None]] = deque()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reader_active = False

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        loop = asyncio.get_running_loop()
        if self._loop is not loop:
            if self._reader_active and self._loop is not None:
                self._loop.remove_reader(self._fd)
                self._reader_active = False
            self._loop = loop
        return loop

    def _start_reader(self) -> None:
        loop = self._ensure_loop()
        if not self._reader_active:
            loop.add_reader(self._fd, self._on_ready)
            self._reader_active = True

    def _stop_reader(self) -> None:
        if self._reader_active and self._loop is not None:
            self._loop.remove_reader(self._fd)
            self._reader_active = False

    def _on_ready(self) -> None:
        loop = self._loop
        if loop is None:
            return
        while self._waiters:
            try:
                os.read(self._fd, 8)
            except BlockingIOError:
                break
            except Exception as exc:  # pragma: no cover - catastrophic failure
                while self._waiters:
                    fut = self._waiters.popleft()
                    if not fut.done():
                        fut.set_exception(exc)
                self._stop_reader()
                return
            fut = self._waiters.popleft()
            if not fut.done():
                fut.set_result(None)
        if not self._waiters:
            self._stop_reader()

    async def wait(self) -> None:
        loop = self._ensure_loop()
        fut: asyncio.Future[None] = loop.create_future()
        self._waiters.append(fut)
        self._start_reader()
        try:
            await fut
        except asyncio.CancelledError:
            if not fut.done():
                fut.cancel()
            try:
                self._waiters.remove(fut)
            except ValueError:
                pass
            if not self._waiters:
                self._stop_reader()
            raise


class Channel:
    """Asynchronous multi-producer multi-consumer channel."""

    def __init__(self, capacity: int, *, slot_size: int = 65536) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if slot_size <= 0:
            raise ValueError("slot_size must be positive")
        self.capacity = capacity
        self._slot_size = slot_size
        self._queue = AsyncQueue(capacity=capacity, slot_size=slot_size)
        self._init_waiters()

    def _init_waiters(self) -> None:
        item_fd, space_fd = self._queue.filenos()
        os.set_blocking(item_fd, False)
        os.set_blocking(space_fd, False)
        self._item_waiter = _FDWaitQueue(item_fd)
        self._space_waiter = _FDWaitQueue(space_fd)

    def __getstate__(self) -> Dict[str, Any]:
        mem_fd, mem_size, items_fd, space_fd = self._queue.share()
        return {
            "capacity": self.capacity,
            "slot_size": self._slot_size,
            "handles": (
                reduction.DupFd(mem_fd),
                mem_size,
                reduction.DupFd(items_fd),
                reduction.DupFd(space_fd),
            ),
        }

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.capacity = state["capacity"]
        self._slot_size = state["slot_size"]
        mem_handle, mem_size, items_handle, space_handle = state["handles"]
        mem_fd = _detach_handle(mem_handle)
        items_fd = _detach_handle(items_handle)
        space_fd = _detach_handle(space_handle)
        self._queue = AsyncQueue.from_handles(mem_fd, mem_size, items_fd, space_fd)
        self._init_waiters()

    @asynccontextmanager
    async def open(self):
        self._queue.add_sender()
        try:
            yield self
        finally:
            self._queue.release_sender()

    async def put(self, item: Any) -> None:
        while True:
            status = self._queue.try_push_object(item)
            if status == 0:
                return
            if status == 1:
                raise ChannelClosed("channel is closed")
            if status != 2:
                raise RuntimeError(f"unexpected status from try_push: {status}")
            await self._wait_for_space()

    async def _wait_for_item(self) -> None:
        await self._item_waiter.wait()

    async def _wait_for_space(self) -> None:
        await self._space_waiter.wait()

    def __aiter__(self) -> "Channel":
        return self

    async def __anext__(self) -> Any:
        while True:
            item, status = self._queue.try_pop_object()
            if status == 0:
                return item
            if status == 1:
                await self._wait_for_item()
                continue
            if status == 2:
                raise StopAsyncIteration
            raise RuntimeError(f"unexpected status from try_pop: {status}")

    def is_closed(self) -> bool:
        return bool(self._queue.is_closed())

    def active_senders(self) -> int:
        return int(self._queue.active_senders())
