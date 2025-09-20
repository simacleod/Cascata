import asyncio
import multiprocessing
import os
import queue
import sys

import pytest

ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

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
    channel = Channel(capacity=10000)
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

    all_items = items1 + items2
    assert len(all_items) == 1000
    assert sorted(all_items) == list(range(1000))
    assert set(items1).isdisjoint(set(items2))


def _stress_worker(iterations: int, result_queue: multiprocessing.Queue) -> None:
    from tests.channel_benchmark import run_benchmark, verify_integrity

    for _ in range(iterations):
        result = run_benchmark(
            producers=2,
            consumers=2,
            messages_per_producer=1,
            capacity=256,
            start_method="spawn",
        )
        ok, error = verify_integrity(result)
        result_queue.put((ok, error))


def test_channel_benchmark_stress():
    workers = 10
    iterations_per_worker = 10
    ctx = multiprocessing.get_context("spawn")
    queue_out: multiprocessing.Queue = ctx.Queue()
    processes = [
        ctx.Process(target=_stress_worker, args=(iterations_per_worker, queue_out))
        for _ in range(workers)
    ]
    for proc in processes:
        proc.start()
    try:
        for _ in range(workers * iterations_per_worker):
            try:
                ok, error = queue_out.get(timeout=30)
            except queue.Empty as exc:  # pragma: no cover - defensive
                pytest.fail("benchmark iteration did not report completion")
                raise AssertionError from exc
            assert ok, error
    finally:
        for proc in processes:
            proc.join(timeout=30)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=5)
                pytest.fail("benchmark stress worker did not exit cleanly")
    for proc in processes:
        assert proc.exitcode == 0, f"benchmark process exited with {proc.exitcode}"
