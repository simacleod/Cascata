# Cascata Runtime Modules

This directory houses the runtime implementation of Cascata.  Each file below is briefly explained along with every
class, function, and constant it defines.

## Table of Contents
- [channel.py](#channelpy)
- [component.py](#componentpy)
- [graph.py](#graphpy)
- [log.py](#logpy)
- [port.py](#portpy)
- [__init__.py](#__initpy)

## channel.py
Implements `Channel`, an asynchronous queue shared across processes.

### Class `Channel`
- `__init__(capacity)` – create shared buffers and control events.
- `open()` – asynchronous context manager signalling an active sender.
- `put(item)` – append an item and notify waiting consumers.
- `__aiter__()` – return the channel itself as an async iterator.
- `__anext__()` – wait for data or closure and yield items until empty.

## component.py
Provides the component framework and helper decorators.

### Class `ComponentMeta`
- `__mul__(count)` – produce a `ComponentGroup` when multiplied.

### Class `Component`
- `__init__(name)` – create port tables and persistence containers.
- `__repr__()` – pretty string showing inports and outports.
- `__getattr__(attr)` – look up ports and persistent values.
- `copy()` – clone the component including ports and values.
- `add_inport(port)` – register an `InputPort`.
- `add_outport(port)` – register an `OutputPort`.
- `add_persist(name, initializer)` – create a `PersistentValue`.
- `set_runner(func)` – attach the coroutine implementing logic.
- `clk(*ports)` – mark ports executed in sequence.
- `sync(*ports)` – group ports to iterate together.
- `_group_listener(group, execute)` – helper that iterates a port group and optionally calls the runner.
- `run()` – open ports, seed defaults, and process all groups.

### Class `ComponentGroup`
- `__init__(comp_cls, count)` – store class and create members later.
- `__repr__()` – human friendly representation.
- `__getattr__(attr)` – proxy for ports on all members.
- `initialize(name, value)` – assign an initial value to all members.
- `connect(src, target)` – wire an output from this group to another port or group, creating a bridge if needed.

### Class `GroupConnector`
- `__init__(base, name, M, N)` – allocate `M` inports and `N` outports.
- `copy()` – return a new connector with the same configuration.
- `run()` – forward items from inputs to outputs in round robin.

### Decorators
- `inport(port_name, capacity=10, default=None)` – declare an input port for a runner.
- `outport(port_name)` – declare an output port for a runner.
- `persist(name, initializer)` – define persistent state for a runner.
- `component(func)` – transform an async function into a `Component` subclass.

## graph.py
Defines the `Graph` container and helpers for sharding and execution.

### Class `DeadlockError`
- `__init__(issues)` – record the detected issues and format the message.

### Class `Graph`
- `__init__()` – initialize node stores and helper structures.
- `__setattr__(name, value)` – intercept attribute assignment for components and subgraphs.
- `__getattr__(name)` – resolve exported ports or group handles.
- `node(name, component)` – add a component instance to the graph.
- `edge(outport, inport)` – connect an outport to an inport.
- `_add_component_group(alias, factory)` – instantiate group members and track them.
- `export(port, name)` – expose a port as an attribute of the graph.
- `_find_root_graph(edge)` – locate the root graph that owns a connection.
- `_walk(graph)` – ascend to the topmost parent graph.
- `_add_subgraph(name, graph)` – clone a graph and attach it as a subgraph.
- `copy(gname)` – deep copy this graph with all components and edges.
- `_break_cycles()` – temporarily remove a cycle for sorting.
- `check_deadlocks()` – static analysis for missing dependencies.
- `shard(num_workers)` – split the graph among worker processes.
- `start(num_workers=None)` – spawn processes and run components.
- `join()` – join all running processes, handling interrupts.
- `run(num_workers=None)` – convenience method calling `start` then `join`.
- `to_dot()` – produce a GraphViz representation.

### Class `GraphWorker`
- `__init__()` – create an empty worker.
- `run()` – run all assigned components in its own event loop.

## log.py
Custom logging utilities providing context-aware output.

### Constants
- `RESET` – ANSI escape sequence to reset color.
- `LEVEL_COLORS` – mapping of log levels to color codes.

### Functions
- `_get_component_color(class_name)` – deterministically choose a bright color.

### Runtime patching
- `_current_component` – context variable storing the active component.
- `_original_run` – original `Component.run` before patching.
- `_run_with_context(self, *args, **kwargs)` – wrapper that sets `_current_component` while running a component.
- The original `Component.run` is replaced with `_run_with_context` so log messages show their source component.

### Class `ContextAwareHandler`
- `emit(record)` – format log messages with the current component name.

### Logger setup
- `log` – module level logger configured with `ContextAwareHandler` at DEBUG level.

## port.py
Primitives used by components to communicate through `Channel` objects.

### Class `InputPort`
- `__init__(name, capacity=100, default=None)` – create the channel and store an optional initial value.
- `__repr__()` – return a human friendly identifier.
- `put(item)` – forward an item to the channel.
- `open()` – async context manager delegating to the channel.
- `initialize(value)` – set the initial value delivered before consumption.
- `__aiter__()` – iterate over items from the channel.
- `__lt__(value)` – syntax sugar for `initialize` in graph DSL.

### Class `OutputPort`
- `__init__(name)` – create an empty connection set.
- `open()` – open all downstream ports concurrently.
- `__repr__()` – identifier similar to `InputPort.__repr__`.
- `send(item)` – send an item to all connected inputs.
- `connect(inport)` – register a connection and notify the graph.
- `__rshift__(inport)` – operator alias for `connect`.

### Class `PortHandler`
- `__init__(name, parent)` – store the port name and owning group.
- `__lt__(value)` – initialize the same port on each component in the group.
- `__rshift__(inport)` – connect every member’s port to a target port.

### Class `PersistentValue`
- `__init__(initializer, *args, **kwargs)` – store the factory callable and arguments.
- `set_args(*args, **kwargs)` – update the initializer arguments and clear the cached value.
- `__lt__(other)` – shorthand for `set_args` in the graph DSL.
- `get()` – lazily initialize and return the stored value.
- `set(value)` – directly provide a value, marking it initialized.

## __init__.py
Convenient re-exports for library consumers.
- Imports `inport`, `outport`, `persist`, and `component` from `component.py`.
- Imports `Graph` from `graph.py` and `log` from `log.py`.
- Defines `__all__` so `from cascata import *` exposes these symbols.
