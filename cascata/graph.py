import copy
import networkx as nx
import multiprocess
import asyncio
import inspect
from .component import Component
from .port import InputPort, OutputPort
import traceback
try:
    from graphviz import Digraph
except:
    Digraph = False


class Graph:

    """Summary

    Attributes:
        edges (list): list of 2-tuples (InputPort,OutputPort)
        networkx_graph (networkx.DiGraph): Internal networkx graph used for distributing components among workers
        nodes (dict): mapping of names to components in the graph
        processes (list): list of processes spawned by run() or start() calls
    """

    def __init__(self):
        self.nodes = {}
        self.edges = []
        self.processes = []
        self.networkx_graph = nx.DiGraph()  # Directed graph
        self._local_namespace = inspect.currentframe().f_back.f_locals

    def __setattr__(self, name, value):
        """setting an attribute with a Component will add it to the graph

        Args:
            name (TYPE): Name of attribute being set
            value (TYPE): If this is a Component, it is added to the graph
        """
        if isinstance(value, type):
            if issubclass(value, Component):
                self.node(name, value(name))
        else:
            super().__setattr__(name, value)

    def node(self, name, component):
        """Adds a named component to the graph.

        Args:
            name (str): name of the component
            component (Component): component to add to the graph.
        """
        component.graph = self
        component.name = name
        self.nodes[name] = component
        self.__dict__[name] = component
        self._local_namespace[name] = component

    def edge(self, outport, inport):
        """Connects an outport to an inport.

        Args:
            outport (cascata.OutputPort): output port to connect
            inport (cascata.InputPort): input port to connect
        """
        outport.connect(inport)
        self.edges.append((outport, inport))
        self.networkx_graph.add_edge(outport.component, inport.component)

    def _break_cycles(self):
        """
        Breaks cycles in the graph for BFS coloring.
        """
        try:
            cycle = nx.find_cycle(self.networkx_graph, orientation='original')
            # Removing an edge to break the cycle
            self.networkx_graph.remove_edge(*cycle[0])
            return True
        except nx.NetworkXNoCycle:
            return False

    def shard(self, num_workers):
        """Shards the graph into a set of GraphWorkers. Breaks all cycles in the internal networkx graph and runs a topological sort. Assigns components to a worker based on the sort order.

        Args:
            num_workers (int): Number of worker processes to spawn

        Returns:
            list: List of GraphWorkers.
        """
        while self._break_cycles():
            pass
        # Perform a topological sort on the graph
        topo_sorted_nodes = list(nx.topological_sort(self.networkx_graph))
        color_map = {}

        # Assign workers based on topological order
        for index, node in enumerate(topo_sorted_nodes):
            color_map[node] = index % num_workers

        workers = [GraphWorker() for _ in range(num_workers)]
        for component_name, component in self.nodes.items():
            worker_index = color_map.get(component_name, 0)
            workers[worker_index].components.append(component)
        return workers

    def start(self, num_workers=None):
        """Starts the graph, without joining it.

        Args:
            num_workers (int, optional): If specified, spawns this amount of processes. If unspecified, uses either the cpu count or the total number of nodes, whichever is smaller.
        """
        if num_workers is None:
            num_workers = min(len(self.nodes), multiprocess.cpu_count())
        workers = self.shard(num_workers)
        self.processes = []
        for worker in workers:
            p = multiprocess.Process(target=worker.run)
            p.start()
            self.processes.append(p)

    def join(self):
        """Joins all processes in the graph. Handles KeyboardInterrupts gracefully.
        """
        try:
            for p in self.processes:
                p.join()
        except KeyboardInterrupt:
            print("\nGraph execution cancelled by user.")
        finally:
            for p in self.processes:
                p.terminate()

    def run(self, num_workers=None):
        """Starts the graph and joins it

        Args:
            num_workers (int, optional): If specified, spawns this amount of processes. If unspecified, uses either the cpu count or the total number of nodes, whichever is smaller.
        """
        self.start(num_workers)
        self.join()

    def to_dot(self):
        """Convert this graph into a GraphViz object for visualization purposes.

        Returns:
            graphviz.Digraph: GraphViz graph object
        """
        if not Digraph:
            print('to_dot requires the "graphviz" module:')
            print('    pip install graphviz')
        dot = Digraph(comment='Cascata Graph', graph_attr=dict(
            rankdir='TD'), node_attr=dict(shape='box'))

        for component_name, component in self.nodes.items():
            # Create a label for the component with inports, component name, and outports
            inports = '|'.join(f'<{port.name}> {port.name}' for port in sorted(component.inports, key=lambda x: x.name))
            outports = '|'.join(f'<{port.name}> {port.name}' for port in sorted(component.outports, key=lambda x: x.name))
            label = f'{{{{ {inports} }}|{component_name}@{component.__class__.__name__}|{{ {outports} }}}}'

            # Add the component node to the graph
            dot.node(component_name, label=label, shape='record')

        # Add edges based on self.edges
        for outport, inport in self.edges:
            dot.edge(f'{inport.component.name}:{inport.name}', f'{outport.component.name}:{outport.name}')

        return dot


class GraphWorker:

    """Main worker for Cascata Graphs.
    Runs all run() methods of all components in the worker asynchronously.

    Attributes:
        components (list): List of components whos run methods will be spawned on this process
    """

    def __init__(self):
        self.components = []

    def run(self):
        """Creates 
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(asyncio.gather(
                *[comp.run() for comp in self.components]))
        except Exception as e:
            print(f"Exception in GraphWorker: {e}")
            traceback.print_exc()
        finally:
            loop.close()
