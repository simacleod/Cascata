import argparse
import asyncio
import multiprocessing as mp
import os
import time
from dataclasses import dataclass

from cascata.channel import Channel


@dataclass
class BenchmarkResult:
    total_messages: int
    elapsed: float
    throughput: float
    per_consumer: list
    consumer_delay: float
    total_overhead: float


async def _producer(
    channel: Channel,
    start: int,
    count: int,
    ready_queue: mp.Queue,
    start_event: mp.Event,
) -> None:
    async with channel.open():
        ready_queue.put(os.getpid())
        await asyncio.to_thread(start_event.wait)
        for value in range(start, start + count):
            await channel.put(value)


async def _consumer(channel: Channel, result_queue: mp.Queue, consumer_delay: float, ready_queue: mp.Queue) -> None:
    ready_queue.put(os.getpid())
    total = 0
    checksum = 0
    async for item in channel:
        total += 1
        checksum += int(item)
        if consumer_delay:
            await asyncio.sleep(consumer_delay)
    result_queue.put((total, checksum))


def producer_entry(
    channel: Channel,
    start: int,
    count: int,
    ready_queue: mp.Queue,
    start_event: mp.Event,
) -> None:
    asyncio.run(_producer(channel, start, count, ready_queue, start_event))


def consumer_entry(channel: Channel, result_queue: mp.Queue, consumer_delay: float, ready_queue: mp.Queue) -> None:
    asyncio.run(_consumer(channel, result_queue, consumer_delay, ready_queue))


def run_benchmark(
    producers: int,
    consumers: int,
    messages_per_producer: int,
    capacity: int,
    start_method: str = "spawn",
    consumer_delay: float = 0.0,
) -> BenchmarkResult:
    ctx = mp.get_context(start_method)
    channel = Channel(capacity=capacity)
    result_queue = ctx.Queue()
    consumer_ready_queue = ctx.Queue()
    producer_ready_queue = ctx.Queue()
    producer_start_event = ctx.Event()

    producer_processes = [
        ctx.Process(
            target=producer_entry,
            args=(
                channel,
                i * messages_per_producer,
                messages_per_producer,
                producer_ready_queue,
                producer_start_event,
            ),
        )
        for i in range(producers)
    ]

    consumer_processes = [
        ctx.Process(
            target=consumer_entry,
            args=(channel, result_queue, consumer_delay, consumer_ready_queue),
        )
        for _ in range(consumers)
    ]

    for proc in consumer_processes:
        proc.start()

    for _ in range(consumers):
        consumer_ready_queue.get()

    for proc in producer_processes:
        proc.start()

    for _ in range(producers):
        producer_ready_queue.get()

    producer_start_event.set()
    start = time.perf_counter()

    for proc in producer_processes:
        proc.join()

    for proc in consumer_processes:
        proc.join()

    elapsed = time.perf_counter() - start

    per_consumer = [result_queue.get() for _ in range(consumers)]
    total_messages = producers * messages_per_producer
    throughput = total_messages / elapsed if elapsed > 0 else 0.0
    total_received = sum(count for count, _ in per_consumer)
    total_overhead = consumer_delay * total_received

    return BenchmarkResult(
        total_messages=total_messages,
        elapsed=elapsed,
        throughput=throughput,
        per_consumer=per_consumer,
        consumer_delay=consumer_delay,
        total_overhead=total_overhead,
    )


def verify_integrity(result: BenchmarkResult) -> tuple[bool, str | None]:
    total_received = sum(count for count, _ in result.per_consumer)
    checksum = sum(chk for _, chk in result.per_consumer)

    expected_count = result.total_messages
    expected_checksum = expected_count * (expected_count - 1) // 2

    if total_received != expected_count:
        return False, f"message count mismatch: expected {expected_count}, received {total_received}"
    if checksum != expected_checksum:
        return False, f"checksum mismatch: expected {expected_checksum}, received {checksum}"
    return True, None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stress-test cascata.channel.Channel")
    parser.add_argument("--producers", type=int, default=os.cpu_count() or 4)
    parser.add_argument("--consumers", type=int, default=os.cpu_count() or 4)
    parser.add_argument("--messages", type=int, default=10000, help="messages per producer")
    parser.add_argument("--capacity", type=int, default=4096)
    parser.add_argument(
        "--start-method",
        choices=("spawn", "fork", "forkserver"),
        default="spawn",
        help="multiprocessing start method",
    )
    parser.add_argument(
        "--skip-integrity-check",
        action="store_true",
        help="skip validating that messages were neither lost nor duplicated",
    )
    parser.add_argument(
        "--consumer-delay",
        type=float,
        default=0.0,
        help="per-message delay injected by consumers to simulate work (in seconds)",
    )

    args = parser.parse_args(argv)

    result = run_benchmark(
        producers=args.producers,
        consumers=args.consumers,
        messages_per_producer=args.messages,
        capacity=args.capacity,
        start_method=args.start_method,
        consumer_delay=args.consumer_delay,
    )

    ok, error = verify_integrity(result)

    print(
        "Benchmark results:\n"
        f"  Producers        : {args.producers}\n"
        f"  Consumers        : {args.consumers}\n"
        f"  Messages/producer: {args.messages}\n"
        f"  Capacity         : {args.capacity}\n"
        f"  Start method     : {args.start_method}\n"
        f"  Total messages   : {result.total_messages}\n"
        f"  Elapsed (s)      : {result.elapsed:.4f}\n"
        f"  Throughput (msg/s): {result.throughput:.2f}\n"
        f"  Consumer delay   : {result.consumer_delay:.4f}\n"
        f"  Total overhead (s): {result.total_overhead:.4f}"
    )

    if ok:
        print("Integrity check   : PASS")
    else:
        print("Integrity check   : FAIL")
        print(f"  Reason          : {error}")
        if not args.skip_integrity_check:
            return 1

    counts = [count for count, _ in result.per_consumer]
    if counts:
        max_count = max(counts)
        min_count = min(counts)
        imbalance = max_count - min_count
        avg = sum(counts) / len(counts)
        print(
            "Consumer distribution:\n"
            f"  Max count        : {max_count}\n"
            f"  Min count        : {min_count}\n"
            f"  Avg count        : {avg:.2f}\n"
            f"  Imbalance (max-min): {imbalance}"
        )
    for idx, (count, checksum) in enumerate(result.per_consumer):
        print(f"  Consumer {idx}: count={count}, checksum={checksum}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
