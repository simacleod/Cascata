import asyncio
from collections import OrderedDict
from contextlib import AsyncExitStack
from aioitertools import zip as azip
from copy import deepcopy
from .port import InputPort, OutputPort


class Component:
    def __init__(self, name):
        self.name = name
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
        
        return f"{'_'*(max_length+4)}\n| {inports_text_centered} |\n| {component_text_centered} |\n| {outports_text_centered} |\n{'¯'*(max_length+4)}"

    def __getattr__(self,attr):
        return self.ports.get(attr)
    
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
            print(self._groups)
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

