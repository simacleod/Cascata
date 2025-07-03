import asyncio
import multiprocessing
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from cascata.channel import Channel

# Helper functions for multiprocessing

def producer_proc(channel, start, end):
    async def _run():
        async with channel.open():
            for i in range(start, end):
                await channel.put(i)
    asyncio.run(_run())

def consumer_proc(channel, queue):
    async def _run():
        items = []
        async for item in channel:
            items.append(item)
        queue.put(items)
    asyncio.run(_run())

def test_channel_multi_process():
    channel = Channel(capacity=1000)
    q1 = multiprocessing.Queue()
    q2 = multiprocessing.Queue()

    p_prod1 = multiprocessing.Process(target=producer_proc, args=(channel, 0, 500))
    p_prod2 = multiprocessing.Process(target=producer_proc, args=(channel, 500, 1000))
    p_cons1 = multiprocessing.Process(target=consumer_proc, args=(channel, q1))
    p_cons2 = multiprocessing.Process(target=consumer_proc, args=(channel, q2))

    # Start consumers first so they wait on the channel
    p_cons1.start()
    p_cons2.start()
    p_prod1.start()
    p_prod2.start()

    p_prod1.join()
    p_prod2.join()
    p_cons1.join()
    p_cons2.join()

    items1 = q1.get()
    items2 = q2.get()

    assert len(items1) == 1000
    assert len(items2) == 1000
    assert sorted(items1) == list(range(1000))
    assert sorted(items2) == list(range(1000))
