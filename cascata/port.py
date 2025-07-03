import asyncio
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
        self.component=None
        self.channel = Channel(capacity)  # Initialize a Channel object
        self.initialization_value = default

    def __repr__(self):
        return f'inport {self.component.name}.{self.name}'

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

    def initialize(self,value):
        """
            initialize this port with a value
        """
        self.initialization_value=value

    def __aiter__(self):
        """
        Makes InputPort an asynchronous iterable by delegating to the channel's iterator.
        """
        return self.channel.__aiter__()

    def __lt__(self,b):
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

    @asynccontextmanager
    async def open(self):
        async with AsyncExitStack() as stack:
            await asyncio.gather(*[stack.enter_async_context(port.open()) for port in self.connections])
            yield

    def __repr__(self):
        return f'inport {self.component.name}.{self.name}'

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
        inport.initialization_value=None
        self.component.graph.edge(self, inport)
        if type(inport) is InputPort:
            self.connections.add(inport)

    def __rshift__(self,inport):
        self.connect(inport)


class PortHandler:
    def __init__(self, name, parent):
        self.name = name
        self.component = parent
    
    def __lt__(self,b):
        self.component.initialize(self.name, b)

    def __rshift__(self,inport):
        self.component.connect(self, inport)


class PersistentValue:
    def __init__(self, initializer, *args, **kwargs):
        """Holds mutable state initialized once per process."""
        self.initializer = initializer
        self._args = args
        self._kwargs = kwargs
        self._value = None
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
            self._value = self.initializer(*self._args, **self._kwargs)
            self._initialized = True
        return self._value

    def set(self, value):
        self._value = value
        self._initialized = True


