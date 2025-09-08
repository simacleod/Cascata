import asyncio
from collections import OrderedDict
from contextlib import AsyncExitStack
from aioitertools import zip as azip, chain, zip_longest, cycle
from copy import deepcopy
from typing import get_type_hints
from .channel import Channel
from .port import InputPort, OutputPort, PortHandler, PersistentValue

# Registry for component metadata
component_registry = {}


def _registry_insert(module_path, component_name, metadata):
    """Insert component metadata into the nested registry."""
    parts = module_path.split(".")
    node = component_registry
    for part in parts:
        node = node.setdefault(part, {})
    node[component_name] = metadata


class ComponentMeta(type):
    def __mul__(cls, count):
        if not issubclass(cls, Component) or not isinstance(count, int):
            return NotImplemented
        return ComponentGroup(cls, count)


class Component(metaclass=ComponentMeta):
    def __init__(self, name):
        self.name = name
        self.group_name = None
        self.ports = {}
        self.graph = None
        self.inports = set()
        self.outports = set()
        self._vals = OrderedDict()  # Holds references to ports and persistent values
        self._persists = OrderedDict()
        self._runner = None
        self._groups = []

    def __repr__(self):
        inports_text = " | ".join(port.name for port in self.inports)
        outports_text = " | ".join(port.name for port in self.outports)
        component_text = self.name

        # Determining the longest string for centering
        max_length = max(len(inports_text), len(component_text), len(outports_text))

        # Centering each line based on the longest string
        inports_text_centered = inports_text.center(max_length)
        component_text_centered = component_text.center(max_length)
        outports_text_centered = outports_text.center(max_length)

        return f"\n{'_'*(max_length+4)}\n| {inports_text_centered} |\n| {component_text_centered} |\n| {outports_text_centered} |\n{'¯'*(max_length+4)}"

    def __getattr__(self, attr):
        return self.ports.get(attr)

    def copy(self):

        new = self.__class__(self.name)

        for ip in self.inports:
            new_ip = getattr(new, ip.name)
            new_ip.initialization_value = ip.initialization_value
            new_ip.batch_size = ip.batch_size

        new._groups = [
            tuple(getattr(new, port.name) for port in group) for group in self._groups
        ]

        for name, pv in self._persists.items():
            new_pv = new._persists.get(name)
            new_pv.set_args(*pv._args, **pv._kwargs)
            if pv._initialized:
                new_pv.set(pv.value)

        return new

    def add_inport(self, port):
        self.inports.add(port)
        port.component = self
        self.ports[port.name] = port
        self._vals[port] = None

    def add_outport(self, port):
        self.outports.add(port)
        port.component = self
        self.ports[port.name] = port
        self._vals[port] = port

    def add_persist(self, name, initializer):
        pv = PersistentValue(initializer)
        self._persists[name] = pv
        self.ports[name] = pv
        self._vals[pv] = pv
        return pv

    def set_runner(self, runner_function):
        self._runner = runner_function

    @classmethod
    def test(cls, **kwargs):
        """Execute the component's runner with provided inport values.

        Keyword arguments must supply a value for every inport of the
        component.  Outports are replaced with dummy ports that simply
        collect the items sent to them.  The collected items are returned
        as a ``dict`` keyed by outport name.

        Example
        -------
        >>> @component
        ... @inport('x')
        ... @outport('y')
        ... async def dup(x, y):
        ...     await y.send(x)
        >>> dup.test(x=1)
        {'y': [1]}
        """

        instance = cls(cls.__name__)

        class _Collector:
            def __init__(self):
                self.items = []

            async def send(self, item):
                self.items.append(item)

        outputs = {}
        args = []
        for port in instance._vals.keys():
            if isinstance(port, InputPort):
                if port.name not in kwargs:
                    raise TypeError(f"Missing value for inport '{port.name}'")
                args.append(kwargs[port.name])
            elif isinstance(port, OutputPort):
                collector = _Collector()
                outputs[port.name] = collector
                args.append(collector)
            elif isinstance(port, PersistentValue):
                port.get()
                args.append(port)

        asyncio.run(instance._runner(*args))

        return {name: collector.items for name, collector in outputs.items()}

    def clk(self, *ports):
        for port in ports:
            self._groups.append((port,))

    def sync(self, *ports):
        self._groups.append((ports))

    async def _group_listener(self, group, execute_runner):
        if execute_runner:

            async def listener(group):
                async for values in azip(*group):
                    self._vals.update(zip(group, values))
                    args = []
                    for v in self._vals.values():
                        if isinstance(v, PersistentValue):
                            v.get()
                        args.append(v)
                    await self._runner(*args)

        else:

            async def listener(group):
                async for values in azip(*group):
                    self._vals.update(zip(group, values))

        await listener(group)

    async def run(self):

        async with AsyncExitStack() as stack:
            await asyncio.gather(
                *[stack.enter_async_context(port.open()) for port in self.outports]
            )
            # Categorize ports based on initialization values
            initports = {
                port for port in self.inports if port.initialization_value is not None
            }

            # Initialize ports with initialization values
            for port in initports:
                if isinstance(port.initialization_value, OutputPort):
                    async for value in port:
                        self._vals[port] = value
                        break
                else:
                    self._vals[port] = port.initialization_value

            # Check if all ports are initports
            if initports == self.inports:
                # If all inports are initports, call the runner and exit
                await self._runner(*self._vals.values())
                return

            if not self._groups:
                self._groups.append(
                    [port for port in self.inports if port.initialization_value is None]
                )

            all_grouped_ports = set().union(*self._groups)
            exec_flags = [True for port in self._groups]
            ungrouped_ports = self.inports - all_grouped_ports - initports

            # Add each ungrouped port as a group on its own
            for port in ungrouped_ports:
                self._groups.append({port})
                exec_flags.append(False)
            # Prepare and run all group listeners concurrently
            group_listeners = [
                self._group_listener(group, flag)
                for group, flag in zip(self._groups, exec_flags)
            ]
            await asyncio.gather(*group_listeners)


def inport(port_name, capacity=10, default=None):
    def decorator(func):
        if not hasattr(func, "_inports"):
            func._inports = []
        func._inports.append((port_name, capacity, default))
        if not hasattr(func, "_val_decls"):
            func._val_decls = []
        func._val_decls.insert(0, ("inport", port_name, capacity, default))
        return func

    return decorator


def outport(port_name):
    def decorator(func):
        if not hasattr(func, "_outports"):
            func._outports = []
        func._outports.append(port_name)
        if not hasattr(func, "_val_decls"):
            func._val_decls = []
        func._val_decls.insert(0, ("outport", port_name))
        return func

    return decorator


def persist(name, initializer):
    def decorator(func):
        if not hasattr(func, "_persists"):
            func._persists = []
        func._persists.append((name, initializer))
        if not hasattr(func, "_val_decls"):
            func._val_decls = []
        func._val_decls.insert(0, ("persist", name, initializer))
        return func

    return decorator


def component(func):
    annotations = get_type_hints(func)

    class ComponentSubclass(Component):
        def __init__(self, *args, **kwargs):
            super().__init__(func.__name__)
            self._persists = OrderedDict()
            for decl in getattr(func, "_val_decls", []):
                kind = decl[0]
                if kind == "inport":
                    _, name, capacity, default = decl
                    port = InputPort(name, capacity, default)
                    port.type = annotations.get(name)
                    self.add_inport(port)
                elif kind == "outport":
                    _, name = decl
                    port = OutputPort(name)
                    port.type = annotations.get(name)
                    self.add_outport(port)
                elif kind == "persist":
                    _, name, init = decl
                    self.add_persist(name, init)
            self.set_runner(deepcopy(func))

    ComponentSubclass.__name__ = func.__name__
    ComponentSubclass.__module__ = func.__module__
    # Register component metadata
    # Use the explicit _inports and _outports declarations captured by the
    # decorators so that the registry retains the port names as well as their
    # properties (e.g. capacity, default value and type annotations).  Prior to
    # this the registry only stored bare type information which meant port names
    # were effectively dropped from the registry making introspection
    # impossible.
    inports_meta = {}
    for name, capacity, default in getattr(func, "_inports", []):
        inports_meta[name] = {
            "type": annotations.get(name),
            "capacity": capacity,
            "default": default,
        }

    outports_meta = {}
    for name in getattr(func, "_outports", []):
        outports_meta[name] = {"type": annotations.get(name)}

    metadata = {
        "module": func.__module__,
        "inports": inports_meta,
        "outports": outports_meta,
    }
    _registry_insert(func.__module__, func.__name__, metadata)

    return ComponentSubclass


class ComponentGroup:
    def __init__(self, comp_cls, count):
        self.comp_cls = comp_cls  # the Component subclass
        self.count = count  # number of copies
        # these get filled in by Graph._add_component_group
        self.name = None
        self.graph = None
        self.group = []  # list of Component instances
        self.ports = {}

    def __repr__(self):
        return f"<ComponentGroup {self.name} x{self.count}>"

    def __getattr__(self, attr):
        if not self.ports:
            for k in self.group[0].ports.keys():
                self.ports[k] = PortHandler(k, self)
        return self.ports[attr]

    def initialize(self, name, val):
        for comp in self.group:
            getattr(comp, name).__lt__(val)

    def connect(self, src, target):
        """
        Connect this group’s output-port `name` into `target`.
        - If `target` is a ComponentGroup of the same size: do 1:1 parallel wiring.
        - Otherwise, insert a M→N GroupConnector under a synthetic name.
        """

        if isinstance(src.component, ComponentGroup):
            M = src.component.count
            src_group = src.component.group
        else:
            src_group = [src.component]
            M = 1

        if isinstance(target.component, ComponentGroup):
            N = target.component.count
            dst_group = target.component.group
        else:
            dst_group = [target.component]
            N = 1

        # Case A: same-size group→group → straight 1:1
        if M == N:
            for srcg, dstg in zip(enumerate(self.group), enumerate(dst_group)):
                i, src_comp = srcg
                j, dst_comp = dstg
                src_comp.ports[src.name] >> dst_comp.ports[target.name]
            return

        # Case B/C: M→N with M != N → insert a bridge
        conn_name = (
            f"{src.component.name}.{src.name}-to-{target.component.name}.{target.name}"
        )
        # create connector component with base port name `name`
        bridge = GroupConnector(conn_name, target.name, M, N)
        # register it in the graph
        self.graph.node(conn_name, bridge)
        # 1) wire each of the M group members into bridge.in_{i}
        for i, src_comp in enumerate(src_group):
            src_comp.ports[src.name] >> bridge.ports[f"{target.name}_{i}_in"]

        # 2) wire each of the N bridge.out_{j} into the dst components
        for j, dst_comp in enumerate(dst_group):
            bridge.ports[f"{target.name}_{j}_out"] >> dst_comp.ports[target.name]

    def connect_batch(self, src, target, size=0):
        """Connect like :py:meth:`connect` but with batching semantics."""
        if isinstance(target, PortHandler):
            if isinstance(target.component, ComponentGroup):
                for comp in target.component.group:
                    getattr(comp, target.name).batch_size = size
            else:
                getattr(target.component, target.name).batch_size = size
        elif isinstance(target, InputPort):
            target.batch_size = size
        else:
            raise TypeError("target must be InputPort or PortHandler")
        self.connect(src, target)


class GroupConnector(Component):
    def __init__(self, base, name, M, N):
        self.name = base
        self.port_name = name
        super().__init__(base)
        self.M = M
        self.N = N
        for i in range(M):
            self.add_inport(InputPort(f"{name}_{i}_in"))
        for j in range(N):
            self.add_outport(OutputPort(f"{name}_{j}_out"))

    def copy(self):
        return GroupConnector(self.name, self.port_name, self.M, self.N)

    async def run(self):
        async with AsyncExitStack() as stack:
            await asyncio.gather(
                *[stack.enter_async_context(port.open()) for port in self.outports]
            )
            in_iters = [
                self.ports[f"{self.port_name}_{i}_in"].__aiter__()
                for i in range(self.M)
            ]
            merged = chain.from_iterable(
                zip_longest(*in_iters, fillvalue=StopAsyncIteration)
            )
            dist = cycle(self.outports)
            async for item in merged:
                if item is StopAsyncIteration:
                    break
                thisport = await anext(dist)
                await thisport.send(item)
