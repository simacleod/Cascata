import asyncio
from collections import OrderedDict
from contextlib import AsyncExitStack
from aioitertools import zip as azip, chain, zip_longest, cycle
from copy import deepcopy
from .channel import Channel
from .port import InputPort, OutputPort, PortHandler




class ComponentMeta(type):
    def __mul__(cls, count):
        if not issubclass(cls, Component) or not isinstance(count, int):
            return NotImplemented
        return ComponentGroup(cls, count)

class Component(metaclass = ComponentMeta):
    def __init__(self, name):
        self.name = name
        self.group_name = None
        self.ports={}
        self.graph=None
        self.inports = set()
        self.outports = set()
        self._vals = OrderedDict()  # Holds references to both inports and outports
        self._runner = None
        self._groups = []
   
    def __repr__(self):
        inports_text = ' | '.join(port.name for port in self.inports)
        outports_text = ' | '.join(port.name for port in self.outports)
        component_text = self.name
        
        # Determining the longest string for centering
        max_length = max(len(inports_text), len(component_text), len(outports_text))
        
        # Centering each line based on the longest string
        inports_text_centered = inports_text.center(max_length)
        component_text_centered = component_text.center(max_length)
        outports_text_centered = outports_text.center(max_length)
        
        return f"\n{'_'*(max_length+4)}\n| {inports_text_centered} |\n| {component_text_centered} |\n| {outports_text_centered} |\n{'¯'*(max_length+4)}"

    def __getattr__(self,attr):
        return self.ports.get(attr)

    def copy(self):

        new = self.__class__(self.name)

        for ip in self.inports:
            new_ip = getattr(new, ip.name)
            new_ip.initialization_value = ip.initialization_value

        new._groups = [
            tuple(getattr(new, port.name) for port in group)
            for group in self._groups
        ]

        return new

    def add_inport(self, port):
        self.inports.add(port)
        port.component=self
        self.ports[port.name]=port
        self._vals[port]=None

    def add_outport(self, port):
        self.outports.add(port)
        port.component=self
        self.ports[port.name]=port
        self._vals[port]=port

    def set_runner(self, runner_function):
        self._runner = runner_function

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
                    await self._runner(*self._vals.values())
        else:
            async def listener(group):
                async for values in azip(*group):
                    self._vals.update(zip(group, values))
        
        await listener(group)

    async def run(self):

        async with AsyncExitStack() as stack:
            await asyncio.gather(*[stack.enter_async_context(port.open()) for port in self.outports])
            # Categorize ports based on initialization values
            initports = {port for port in self.inports if port.initialization_value is not None}
    
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
                self._groups.append([port for port in self.inports if port.initialization_value is None])
    
            all_grouped_ports = set().union(*self._groups)
            exec_flags=[True for port in self._groups]
            ungrouped_ports = self.inports-all_grouped_ports-initports
    
            # Add each ungrouped port as a group on its own
            for port in ungrouped_ports:
                self._groups.append({port})
                exec_flags.append(False)
            # Prepare and run all group listeners concurrently
            group_listeners = [self._group_listener(group,flag) for group,flag in zip(self._groups,exec_flags)]
            await asyncio.gather(*group_listeners)


def inport(port_name, capacity=10, default=None):
    def decorator(func):
        if not hasattr(func, '_inports'):
            func._inports = []
        func._inports.append((port_name, capacity, default))
        return func
    return decorator

def outport(port_name):
    def decorator(func):
        if not hasattr(func, '_outports'):
            func._outports = []
        func._outports.append(port_name)
        return func
    return decorator

def component(func):
    class ComponentSubclass(Component):
        def __init__(self, *args, **kwargs):
            super().__init__(func.__name__)
            for args in getattr(func, '_inports', []):
                self.add_inport(InputPort(*args))
            for port_name in getattr(func, '_outports', []):
                self.add_outport(OutputPort(port_name))
            self.set_runner(deepcopy(func))

    ComponentSubclass.__name__ = func.__name__
    return ComponentSubclass

class ComponentGroup:
    def __init__(self, comp_cls, count):
        self.comp_cls = comp_cls     # the Component subclass
        self.count    = count        # number of copies
        # these get filled in by Graph._add_component_group
        self.name     = None
        self.graph    = None
        self.group    = []           # list of Component instances
        self.ports    = {}

    def __repr__(self):
        return f"<ComponentGroup {self.name} x{self.count}>"

    def __getattr__(self, attr):
        if not self.ports:
            for k,v in self.group[0].ports.items():
                self.ports[k]=PortHandler(k, self)
        return self.ports[attr]

    def initialize(self, name, val):
        for comp in self.group:
            comp.ports[name].__lt__(val)

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
                i,src_comp = srcg
                j,dst_comp = dstg
                src_comp.ports[src.name] >> dst_comp.ports[target.name]
            return

        # Case B/C: M→N with M != N → insert a bridge
        conn_name = f"{src.component.name}.{src.name}-to-{target.component.name}.{target.name}"
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


class GroupConnector(Component):
    def __init__(self,base,name,M,N):
        self.name = base
        self.port_name = name
        super().__init__(base)
        self.M = M
        self.N = N
        for i in range(M): self.add_inport(InputPort(f"{name}_{i}_in"))
        for j in range(N): self.add_outport(OutputPort(f"{name}_{j}_out"))

    def copy(self):
        return GroupConnector(self.name, self.port_name, self.M, self.N)

    async def run(self):
        async with AsyncExitStack() as stack:
            await asyncio.gather(*[stack.enter_async_context(port.open()) for port in self.outports])
            in_iters = [self.ports[f"{self.port_name}_{i}_in"].__aiter__() for i in range(self.M)]
            merged   = chain.from_iterable(zip_longest(*in_iters, fillvalue=StopAsyncIteration))
            dist     = cycle(self.outports)
            async for item in merged:
                if item is StopAsyncIteration: break
                thisport = await anext(dist)
                await thisport.send(item)
