import asyncio
from typing import Union, get_args, get_origin
from .channel import Channel
from contextlib import asynccontextmanager, AsyncExitStack


class InputPort:
    def __init__(self, name, capacity=100, default=None):
        """
        Initializes an InputPort.

        :param name: The name of the InputPort.
        :param initialization_value: The initial value for the port, if any.
        """
        self.name = name
        self.component = None
        self.capacity = capacity
        self.channel = Channel(capacity)  # Initialize a Channel object
        self.initialization_value = default
        self.batch_size = None
        self.type = None

    def __repr__(self):
        return f"inport {self.component.name}.{self.name}"

    async def put(self, item):
        """
        Puts an item into the channel.

        :param item: The item to be put into the channel.
        """
        await self.channel.put(item)

    @asynccontextmanager
    async def open(self):
        """
        Context manager for managing the state of the channel.
        """
        async with self.channel.open() as channel:
            yield channel

    def initialize(self, value):
        """
        initialize this port with a value
        """
        self.initialization_value = value

    def __aiter__(self):
        """
        Makes InputPort an asynchronous iterable.
        If batch_size is None, items are yielded individually.
        Otherwise, items are collected and yielded as lists.
        """
        if self.batch_size is None:
            return self.channel.__aiter__()

        async def _gen():
            batch = []
            async for item in self.channel:
                batch.append(item)
                if self.batch_size and len(batch) >= self.batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

        return _gen()

    def __lt__(self, b):
        self.initialize(b)


class OutputPort:
    def __init__(self, name):
        """
        Initializes an OutputPort.

        :param name: The name of the OutputPort.
        """
        self.name = name
        self.component = None
        self.connections = set()  # List to store connections to InputPorts
        self.type = None

    @asynccontextmanager
    async def open(self):
        async with AsyncExitStack() as stack:
            await asyncio.gather(
                *[stack.enter_async_context(port.open()) for port in self.connections]
            )
            yield

    def __repr__(self):
        return f"inport {self.component.name}.{self.name}"

    async def send(self, item):
        """
        Concurrently sends an item to all connected InputPorts.

        :param item: The item to be sent.
        """
        await asyncio.gather(*(connection.put(item) for connection in self.connections))

    def connect(self, inport):
        """
        Connects this OutputPort to an InputPort.

        :param inport: The InputPort to connect to.
        """
        inport.initialization_value = None
        if getattr(self, "type", None) is not None and getattr(inport, "type", None) is not None:
            out_origin = get_origin(self.type)
            out_types = (
                set(get_args(self.type)) if out_origin is Union else {self.type}
            )
            in_origin = get_origin(inport.type)
            in_types = (
                set(get_args(inport.type)) if in_origin is Union else {inport.type}
            )

            for o in out_types:
                if not any(issubclass(o, i) for i in in_types):
                    raise TypeError(
                        f"Cannot connect {self} ({self.type}) to {inport} ({inport.type})"
                    )
        self.component.graph.edge(self, inport)
        if type(inport) is InputPort:
            self.connections.add(inport)

    def connect_batch(self, inport, size=0):
        """Connect with batching semantics."""
        inport.batch_size = size
        self.connect(inport)

    def __rshift__(self, other):
        if isinstance(other, int):
            return _BatchBuilder(self, other)
        self.connect(other)
        return self

    def __irshift__(self, inport):
        self.connect_batch(inport, 0)
        return self


class _BatchBuilder:
    def __init__(self, outport, size):
        self.outport = outport
        self.size = size

    # allow syntax outport >> n >= inport
    def __ge__(self, inport):
        self.outport.connect_batch(inport, self.size)
        return self.outport


class _GroupBatchBuilder:
    """Helper for batching connections from ComponentGroup ports."""

    def __init__(self, handler, size):
        self.handler = handler
        self.size = size

    def __ge__(self, inport):
        self.handler.component.connect_batch(self.handler, inport, self.size)
        return self.handler


class PortHandler:
    def __init__(self, name, parent):
        self.name = name
        self.component = parent

    def __lt__(self, b):
        self.component.initialize(self.name, b)

    def __rshift__(self, other):
        if isinstance(other, int):
            return _GroupBatchBuilder(self, other)
        self.component.connect(self, other)
        return self

    def __irshift__(self, inport):
        self.component.connect_batch(self, inport, 0)
        return self


class PersistentValue:
    def __init__(self, initializer, *args, **kwargs):
        """Holds mutable state initialized once per process."""
        self.initializer = initializer
        self._args = args
        self._kwargs = kwargs
        self.value = None
        self._initialized = False

    def set_args(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._initialized = False

    def __lt__(self, other):
        if isinstance(other, tuple):
            if len(other) == 2 and isinstance(other[1], dict):
                args, kwargs = other
                self.set_args(*args, **kwargs)
            else:
                self.set_args(*other)
        elif isinstance(other, dict):
            self.set_args(**other)
        else:
            self.set_args(other)

    def get(self):
        if not self._initialized:
            self.value = self.initializer(*self._args, **self._kwargs)
            self._initialized = True
        return self.value

    def set(self, value):
        self.value = value
        self._initialized = True
