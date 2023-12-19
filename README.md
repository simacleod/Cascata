

<div align=center> <img src="logo.png" width=33%>

# Cascata 

Parallel graph execution framework for Python

---

</div>


## Introduction
	
Cascata is an asynchronous, event-driven, parallel graph execution framework for Python, powered by [asyncio](https://docs.python.org/3/library/asyncio.html) and [multiprocess](https://github.com/uqfoundation/multiprocess). Cascata enables the creation of networks of components, each operating in their own coroutine. Components in a Cascata graph can run on one or more CPU cores, enabling high-performance applications which benefit from parallel and/or asynchronous execution. 

### Features
- Parallel, asynchronous graph execution
- Declarative component and graph definition DSL
- Automatic workload distribution
- Configurable port iteration and synchronization 




## Quick-start guide

### Install Cascata

Install cascata with pip:

```bash
pip3 install cascata
```

### Define components
Components in Cascata are defined with three decorators: `@inport`, `@outport`, and `@component`. These decorators are applied to an async function definition, transforming the definition into a Component object. This function accepts the same arguments as the ports. The data from the ports, not the ports themselves, are provided as arguments to the function at runtime. Iteration over the ports is handled internally by the component in a user-defined pattern when building the graph. Thus, the runner function needs only to process data, and send data.

```python
from cascata import * #imports the @inport, @outport, @component decorators, and the Graph object.



@component # must be at the top of the declaration
@inport('data_in', capacity=10) # Define an InputPort named "data_in", with a capacity of 10.
@inport('constant_in', default=1) # Define an InputPort which is initialized with a default value.
@outport('data_out') #Define an OutputPort, used for sending data to downstream components.
async def NumberIncrementer(data_in, constant_in, data_out): # Always use async def when defining component runners.

	await data_out.send(data_in + constant_in) # Send the sum of the values of the data received by the inports through the output port

@component
@inport('range_in')
@outport('data_out')
async def RangeGenerator(range_in, data_out):

	for i in range(range_in):
		await data_out.send(i) # Send an item through the output port

@component
@inport('data_in')
async def DataPrinter(data_in):

	print(data_in)


```

### Build and run a graph

Graphs in Cascata are defined with a simple shorthand DSL:
- the assignment operator `=` is used to add components to a graph : `my_graph.my_components_name = component_1`
- assigning a component to a graph will inject its name into the local namespace.
- the less-than operator `<` is used to initialize ports of a component: `increment_numbers_instance.constant_in < 2`
- the right-shift operator `>>` is used to connect components: `increment_numbers_instance.data_out >> print_data.data_in`

```python
from cascata import * #imports the @inport, @outport, @component decorators, and the Graph object.

graph=Graph()

#add our previously defined components in the graph

graph.arange=RangeGenerator 
graph.add_numbers=NumberIncrementer
graph.print_results=DataPrinter

# Doing this injects the names "arange", "add_numbers",
# and "print_results into the local namespace"

#set initial value of the arange.range_in port

arange.range_in < 10 #arange is available in the local namespace

#set initial value of the add_numbers component

add_numbers.constant_in < 5

# define the connections of the graph

arange.data_out >> add_numbers.data_in #outport >> inport
add_numbers.data_out >> print_results.data_in

# run the graph

graph.run(num_workers=3) # optionally specify a number of workers
#by default, uses multiprocess.cpu_count() or the number of components in the graph, whichever is lower.



```

### Other notes and advanced usage:

Components have two methods for defining the iteration pattern of a component, `sync()` and `clk()`.
- `some_component.clk(some_component.some_inport)` will instruct the component to execute its runner only when data is recieved on the specified port.
- `some_component.sync(some_component.some_inport, some_component.another_inport)` is the same as `clk()`, except the component will execute the runner on the zipped iteration of both ports, treating them as a single clocked group.
- by default, all ports use the sync behavior unless they are initialized with a static value.
- multiple groups of ports can be specified with `clk` and `sync`.




see the examples folder for more advanced usage.

## Internals of Cascata

### Channels

At its core, Cascata uses a `Channel` primitive to facilitate communication between components. A complete, running graph in Cascata may be regarded as a network of functions which produce and consume data via iteration over these channels. 

A `Channel` is defined as a thread-safe, non-blocking queue, which provides an `open()` `AsyncContextManager` to signal the number of active publishers, an awaitable `put()` method, and an `__aiter__`/`__anext__` asynchronous iterator which takes from the channel until no more `open()` contexts are active, and no more items remain in the queue. Channels are capable of a "many-to-many" publisher/consumer pattern.

```python

channel_nine=Channel()

async with open(channel_nine) as ch:
	await channel_nine.put(something)

# In another coroutine:

async for item in channel_nine:
	do_something_to(item)

```

### Components and Ports

Ports in Cascata are resposible for transmitting and receiving data through channels. InputPorts each contain their own Channel, and OutputPorts contain a list of InputPorts to which data sent through the OutputPort is transmitted.

Components in Cascata contain one or more InputPort or OutputPort objects and an awaitable runner function. A component iterates over groups of these ports and executes its runner on the incoming data in a user-defined pattern. A component terminates when all of its iterators have exited. 

### Graphs

Graphs in Cascata are used to facilitate *composition* of components into interconnected networks and run them in a group of workers. They are not responsible for any state management, communication, or monitoring of the components themselves. Graphs in Cascata are simply containers of components and their connections.

## Performance considerations

- Cascata uses an asynchronous version of `multiprocessing.Queue` provided by `aioprocessing`  for inter-process communication, which serializes objects with `dill`. Use of shared memory for transporting large amounts of data between processes is highly encouraged. 
- Since all components are defined as coroutines, the full feature set of `asyncio` can be leveraged to accelerate I/O bound code.

## Requirements and dependencies:
- Python 3.7 or 3.8
- Currently working only on Linux.
- Python package dependencies: 
```bash
	aioitertools
	aioprocessing
	multiprocess
	networkx
	graphviz
```

## Acknowlegements

The component definition syntax for Cascata, as well as a large portion of the ontology of the code itself, is inspired by [Rill](https://github.com/PermaData/rill).

Cascata draws its name from the beautiful language of the mostly penninsular nation of Italy. Cascata means "Waterfall" in Italian.

## Contributions and future development

Please report any issues you find with Cascata in the issues page on this repository. 

Contributions to Cascata are highly encouraged and merge requests welcomed.

### Features to be implemented:
	
- Windows, MacOS support
- Support for Subgraphs
- Support for grouped components
- Python3.9+ support
- Deadlock detection
- Logging facilities

If you are a large corporation, and you find Cascata useful in your highly profitable software development ventures, consider sending the author like a million dollars. TIA

## License

Cascata is released under the [MIT License](https://opensource.org/licenses/MIT). This license permits free use, modification, distribution, and private use of Cascata, but with limited liability and warranty as detailed in the license terms.

## Citing Cascata

If you use Cascata in your research, academic projects, or publications, we would appreciate citing it. You can use the following BibTeX entry:

```bibtex
@misc{cascata2023,
  title={Cascata: Parallel Graph Execution Framework for Python},
  author={Stuart MacLeod},
  year={2023},
  howpublished={\url{https://github.com/simacleod/cascata}}
}
```
