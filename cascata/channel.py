import asyncio
import multiprocessing
from contextlib import asynccontextmanager
from multiprocessing import Manager

class Channel:

    def __init__(self, capacity):
        self.capacity = capacity
        mgr = Manager()
        self._items = mgr.list()
        self.sender_count = multiprocessing.Value('i', 0)
        self._start_event = multiprocessing.Event()
        self._close_event = multiprocessing.Event()
        self._item_event = multiprocessing.Event()
        self._sem = multiprocessing.Semaphore(capacity)
        self._buf = []
        self._idx = 0

    @asynccontextmanager
    async def open(self):
        with self.sender_count.get_lock():
            self.sender_count.value += 1
            if self.sender_count.value == 1:
                self._start_event.set()
        try:
            yield self
        finally:
            with self.sender_count.get_lock():
                self.sender_count.value -= 1
                if self.sender_count.value == 0:
                    self._close_event.set()
                    self._item_event.set()

    async def put(self, item):
        if not self._sem.acquire(False):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._sem.acquire)
        self._items.append(item)
        self._item_event.set()

    def __aiter__(self):
        self._buf = []
        self._idx = 0
        return self

    async def __anext__(self):
        loop = asyncio.get_running_loop()
        if not self._start_event.is_set():
            await loop.run_in_executor(None, self._start_event.wait)
        if self._buf:
            item = self._buf.pop(0)
            self._sem.release()
            return item
        new_items = self._items[self._idx:]
        if new_items:
            self._idx += len(new_items)
            self._buf = new_items
            item = self._buf.pop(0)
            self._sem.release()
            return item
        if self._close_event.is_set():
            raise StopAsyncIteration
        self._item_event.clear()
        await loop.run_in_executor(None, self._item_event.wait)
        return await self.__anext__()
