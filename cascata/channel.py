import asyncio
import pickle
import multiprocessing
import time
from contextlib import asynccontextmanager
from multiprocessing import Manager

class Channel:

    def __init__(self, capacity):
        mgr = Manager()
        self._items = mgr.list()
        self.sender_count = multiprocessing.Value('i', 0)
        self._start_event = multiprocessing.Event()
        self._close_event = multiprocessing.Event()
        self._item_event = multiprocessing.Event()
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
            return self._buf.pop(0)
        new_items = self._items[self._idx:]
        if new_items:
            self._idx += len(new_items)
            self._buf = new_items
            return self._buf.pop(0)
        if self._close_event.is_set():
            raise StopAsyncIteration
        self._item_event.clear()
        async def _wait():
            while True:
                if self._item_event.is_set() or self._close_event.is_set():
                    return
                await asyncio.sleep(0.001)
        await _wait()
        return await self.__anext__()
