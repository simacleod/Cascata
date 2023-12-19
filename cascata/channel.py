import asyncio
from aioprocessing import AioQueue as Queue, AioEvent as Event
from multiprocess import Value
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
import multiprocess
import time

class Channel:
    """
    An asynchronous communication channel class that uses asyncio with multiprocess support
    to facilitate inter-process communication.

    Attributes:
        sender_count (multiprocess.Value): A shared value to track the number of active senders.
        active (aioprocessing.AioEvent): An event that receivers wait on until at least one sender is active.
        cancel_event (aioprocessing.AioEvent): An event to signal cancellation across processes.
        cancel_water (future) : A running task which waits on self.cancel_event
        queue (aioprocessing.AioQueue): An asynchronous queue for messages with a specified capacity.
    """
    def __init__(self, capacity=100):
        """
        Initializes a new Channel instance with a specified queue capacity.

        Args:
            capacity (int): The maximum number of items the queue can hold.
        """
        self.sender_count = Value('i', 0)
        self.active = Event()
        self.cancel_event = Event()
        self.cancel_waiter = None
        self.queue = Queue(capacity)

    async def put(self, item):
        """
        An asynchronous coroutine to add an item to the queue.

        Args:
            item: The item to add to the queue.
        """
        await self.queue.coro_put(item)

    @asynccontextmanager
    async def open(self):
        """
        An asynchronous context manager to track the number of active senders.

        Usage:
            async with channel.open():
                await channel.put(item)
        """
        try:
            with self.sender_count.get_lock():
                self.sender_count.value += 1
            self.active.set()
            yield self
        finally:
            with self.sender_count.get_lock():
                self.sender_count.value -= 1


    async def wait_for_cancel(self):
        """
        An asynchronous coroutine that blocks until the cancel_event is set.
        Polls the event status with a timeout.
        """
        while not self.cancel_event.is_set():
            try:
                await asyncio.wait_for(self.cancel_event.coro_wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

    async def get(self):
        """
        An asynchronous coroutine that retrieves an item from the queue or
        raises a StopAsyncIteration if the channel is signaled for cancellation.

        Returns:
            The retrieved item from the queue.

        Raises:
            StopAsyncIteration: If the cancellation event is flagged.
        """
        futures = [
            asyncio.ensure_future(self.queue.coro_get()), 
            self.cancel_waiter
            #asyncio.ensure_future(self.wait_for_cancel())
        ]
        done, pending = await asyncio.wait(
            futures,
            return_when=asyncio.FIRST_COMPLETED
        )

        # Cancel any pending tasks
        if futures[1].done():
            for task in pending:
                task.cancel()
            await asyncio.sleep(0.01)
            raise StopAsyncIteration
        

        for task in done:
            if task.exception():
                raise task.exception()
            return task.result()

    def __aiter__(self):
        """
        Makes the Channel instance an asynchronous iterable.

        Returns:
            The channel instance itself.
        """
        if self.cancel_waiter is None: 
            self.cancel_waiter=asyncio.ensure_future(self.wait_for_cancel())
        return self

    async def flag_cancellation(self):
        """
        An asynchronous coroutine that sets the cancellation event flag,
        indicating that no more messages should be sent or received.
        """
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, self.cancel_event.set)

    async def __anext__(self):
        """
        An asynchronous iterator method to retrieve the next item from the channel's queue.

        Returns:
            The next item from the queue.

        Raises:
            StopAsyncIteration: If there are no active senders and the queue is empty.
        """
        while True:
            await self.active.coro_wait()
            if self.sender_count.value == 0 and self.queue.empty():
                await asyncio.sleep(0.01)
                self.cancel_event.set()

                await asyncio.sleep(0.01)
                raise StopAsyncIteration
            else:
                future = asyncio.ensure_future(self.get())

                try:
                    await asyncio.wait_for(future, timeout=1)
                    if future.done():
                        return future.result()
                        break
                except asyncio.TimeoutError:
                    continue