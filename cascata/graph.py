import copy
import networkx as nx
import multiprocess
import asyncio
import inspect
from .component import Component, ComponentGroup, GroupConnector
from .port import InputPort, OutputPort
from .log import log
import traceback
try:
    from graphviz import Digraph
except:
    Digraph = False

class DeadlockError(Exception):
    """
    Exception raised when deadlock conditions are detected in a Cascata Graph.

    Attributes:
        issues (list[str]): A list of descriptive deadlock issues found.
    """
    def __init__(self, issues: list[str]):
        self.issues = issues
        message = "Deadlock detected:\n" + "\n".join(f"- {issue}" for issue in issues)
        super().__init__(message)


class Graph:
    """Summary

    Attributes:
        nodes (dict): mapping of names to Component instances
        edges (list): list of tuples (OutputPort, InputPort)
        processes (list): list of processes spawned by run() or start()
        networkx_graph (networkx.DiGraph): internal graph for scheduling
        _local_namespace (dict): namespace where Graph was created
        _exports (dict): mapping of export name to Port
    """
    def __init__(self):
        self.nodes = {}
        self.edges = []
        self.processes = []
        self.networkx_graph = nx.DiGraph()
        self._local_namespace = inspect.currentframe().f_back.f_locals
        self._exports = {}
        self.is_subgraph = False
        self.parent = None
        self.group_handles: dict[str,ComponentGroup] = {}

    def __setattr__(self, name, value):
        # detect adding a subgraph instance
        if isinstance(value, Graph):
            self._add_subgraph(name, value)
        # detect adding a Component subclass or instance
        elif isinstance(value, type) and issubclass(value, Component):
            comp = value(name)
            self.node(name, comp)
        elif isinstance(value, Component):
            self.node(name, value)
        elif isinstance(value, ComponentGroup):
            return self._add_component_group(name, value)            
        else:
            super().__setattr__(name, value)

    def __getattr__(self,name):
        if name in self._exports:
            return self._exports[name]
        if name in self.group_handles:
            return self.group_handles[name]            
        else:
            raise AttributeError(f"Graph object has no attribute {name}")

    def node(self, name, component):
        """Adds a named component to the graph."""
        component.graph = self
        component.name = name
        self.nodes[name] = component
        self.__dict__[name] = component
        self._local_namespace[name] = component

    def edge(self, outport, inport):
        """Connects an outport to an inport."""

        if type(inport.component) is ComponentGroup:
            inport.component.connect(outport, inport)
            return
        root = self._find_root_graph((inport,outport))
        root.edges.append((outport, inport))
        root.networkx_graph.add_edge(outport.component, inport.component)
    
    def _add_component_group(self, alias: str, factory: ComponentGroup):
        """
        Instantiate factory.count copies of factory.comp_cls,
        register them under names alias_0…alias_{n-1},
        record them in self.group_handles and networkx_graph.
        """
        factory.name  = alias
        factory.graph = self

        # 1) instantiate all members
        for i in range(factory.count):
            comp_name = f"{alias}_{i}"
            comp = factory.comp_cls(comp_name)
            comp.name = comp_name
            comp.group_name = alias
            comp.graph = self
            self.nodes[comp_name] = comp
            self.__dict__[comp_name] = comp

            self.networkx_graph.add_node(comp, group=(alias, i, factory.count))

            factory.group.append(comp)
        # 2) remember the group handle
        self.group_handles[alias] = factory

        # 3) expose the alias in local namespace
        self._local_namespace[alias] = factory

    def export(self, port, name: str):
        """Expose an internal port as if it were a port on this Graph."""
        if name in self._exports:
            raise ValueError(f"export name '{name}' already used")
        self._exports[name] = port

    def _find_root_graph(self,edge):
        inport,outport = edge
        in_graph = inport.component.graph
        out_graph = outport.component.graph
        ingraph,inlevel = self._walk(in_graph)
        outgraph,outlevel = self._walk(out_graph)
        if inlevel>outlevel:
            return ingraph
        else:
            return outgraph


    def _walk(self, graph):
        level = 0
        cur = graph
        while cur.parent is not None:
            cur = cur.parent
            level += 1
        return cur, level


    def _add_subgraph(self, name, graph):
        new = graph.copy(name)
        new.is_subgraph = True
        new.__dict__['parent'] = self
        self.nodes = {**self.nodes, **new.nodes}
        self.edges += new.edges
        self.networkx_graph=nx.union(self.networkx_graph, new.networkx_graph)
        self._local_namespace[name] = new
        self.__dict__[name]=new

    def copy(self,gname) -> "Graph":
        """
        Return a deep copy of this Graph, cloning all Components and Ports,
        rebuilding edges.
        """
        new = Graph()

        comp_map: dict[Component, Component] = {}
        new.nodes = {}
        for name, comp in self.nodes.items():
            name = f'{gname}_{name}'
            new_comp = comp.copy()
            new_comp.name = name
            new_comp.graph = new
            new.nodes[name] = new_comp
            comp_map[comp] = new_comp
            new.__dict__[name] = new_comp
            new._local_namespace[name] = new_comp

        new.edges = []
        new.networkx_graph = nx.DiGraph()
        for outp, inp in self.edges:
            cloned_out_comp = comp_map[outp.component]
            cloned_in_comp  = comp_map[inp.component]
            new_outp = getattr(cloned_out_comp, outp.name)
            new_inp  = getattr(cloned_in_comp,  inp.name)
            new_outp.connect(new_inp)

        new._exports = {}
        for export_name, port in self._exports.items():
            cloned_comp = comp_map[port.component]
            cloned_port = getattr(cloned_comp, port.name)
            new._exports[export_name] = cloned_port

        return new


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

    def check_deadlocks(self) -> None:
        """
        Static analysis for deadlocks. Raises DeadlockError if any of:
          1) A cycle of components with no seeded inport
          2) Any inport that has neither a default nor an upstream
          3) Any port used as a trigger (clk/sync) that has no upstream
          4) Any exported inport that is never wired
        """
        issues: list[str] = []

        # Build mapping: InputPort -> [outgoing OutputPort, ...]
        port_to_upstreams: dict[InputPort, list] = {}
        for outp, inp in self.edges:
            port_to_upstreams.setdefault(inp, []).append(outp)

        # 1) Cycles lacking a default seed:
        for scc in nx.strongly_connected_components(self.networkx_graph):
            if len(scc) > 1 or any(self.networkx_graph.has_edge(n, n) for n in scc):
                # ensure at least one component in the cycle has an inport.initialization_value
                seeded = False
                for comp_name in scc:
                    comp = self.nodes[comp_name]
                    for ip in comp.inports:
                        if ip.initialization_value is not None:
                            seeded = True
                            break
                    if seeded:
                        break
                if not seeded:
                    issues.append(
                        f"Cycle deadlock: components {sorted(scc)} form a cycle with no default seed."
                    )

        # 2) Orphan inports (no default, no upstream):
        for comp_name, comp in self.nodes.items():
            for ip in comp.inports:
                if ip.initialization_value is None and not port_to_upstreams.get(ip):
                    issues.append(
                        f"Orphan port: '{comp_name}.{ip.name}' has no producer and no default."
                    )

        # 3) Unused clk/sync triggers:
        # comp._groups is a list of tuples of ports used as triggers
        for comp_name, comp in self.nodes.items():
            for group in comp._groups:
                for port in group:
                    if port in comp.inports and not port_to_upstreams.get(port):
                        issues.append(
                            f"Trigger deadlock: '{comp_name}.{port.name}' marked as clk/sync but never fed."
                        )

        # 4) Unconnected exported inports:
        for export_name, port in self._exports.items():
            if isinstance(port, InputPort) and not port_to_upstreams.get(port):
                issues.append(
                    f"Export deadlock: exported inport '{export_name}' has no upstream."
                )

        if issues:
            raise DeadlockError(issues)

    def shard(self, num_workers):
        """
        Shards the graph into GraphWorkers, using the advanced
        shard_graph_final2 strategy to assign each node to a worker.
        """
        # 1) Break all cycles so we can topologically sort
        while self._break_cycles():
            pass
    
        G = self.networkx_graph
    
        # 2) Build a helper map: group name → list of member nodes
        group_map = {}
        for node, data in G.nodes(data=True):
            grp = data.get('group')
            if grp:
                name = grp[0]
                group_map.setdefault(name, []).append(node)
    
        # 3) Topological sort
        topo = list(nx.topological_sort(G))
    
        # 4) Advanced worker‐assignment
        color_map = {}
        rr = 0
        for node in topo:
            # Unpack group info if any
            grp = G.nodes[node].get('group')
            if grp:
                name, idx, size = grp
                members = group_map[name]
    
                # (1) Parallel perfect‐matching: reuse parent’s worker
                preds = list(G.predecessors(node))
                if len(preds) == 1 and len(members) <= num_workers:
                    parents = [next(G.predecessors(m)) for m in members]
                    if len(set(parents)) == len(members):
                        color_map[node] = color_map[parents[idx]]
                        continue
    
            # (2) Split stage 1→N
            preds = list(G.predecessors(node))
            if len(preds) == 1:
                p = preds[0]
                children = list(G.successors(p))
                if len(children) > 1:
                    grp_c = G.nodes[node].get('group')
                    if grp_c and grp_c[2] == len(children):
                        i = children.index(node)
                        pw = color_map[p]
                        if len(children) < num_workers:
                            avail = [w for w in range(num_workers) if w != pw]
                            color_map[node] = avail[i % len(avail)]
                        else:
                            color_map[node] = i % num_workers
                        continue
    
            # (3) Broadcast stage M→workers
            assigned = False
            for p in preds:
                children = list(G.successors(p))
                if len(children) == num_workers:
                    i = children.index(node)
                    color_map[node] = i
                    assigned = True
                    break
            if assigned:
                continue
    
            # (4) Fallback: global round-robin
            color_map[node] = rr
            rr = (rr + 1) % num_workers
    
        # 5) Instantiate workers and assign components
        workers = [GraphWorker() for _ in range(num_workers)]
        for comp_name, comp in self.nodes.items():
            worker_index = color_map.get(comp_name, 0)
            workers[worker_index].components.append(comp)
        return workers

    def start(self, num_workers=None):
        """Starts the graph, without joining it.

        Args:
            num_workers (int, optional): If specified, spawns this amount of processes. If unspecified, uses either the cpu count or the total number of nodes, whichever is smaller.
        """
        if num_workers is None:
            num_workers = min(len(self.nodes), multiprocess.cpu_count())

        self.check_deadlocks()
        
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
            log.warn("\nGraph execution cancelled by user.")
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
        """
        Dark‐themed GraphViz with HTML labels and bright red borders:
        - Inports on top
        - Component name in middle
        - Outports on bottom, with equal column spans
        """
        if not Digraph:
            log.warn('to_dot requires graphviz:\n    pip install graphviz')
            return

        dot = Digraph(
            comment='Cascata Graph (red-border)',
            format='png',
            graph_attr={'rankdir':'TD','bgcolor':'black', 'dpi':'300', 'nodesep':'1.0' ,'ranksep':'1.0'},
            node_attr={'shape':'none','margin':'0'},
            edge_attr={'color':'#FFFFFF'},
        )

        for name, comp in self.nodes.items():
            inps  = sorted(comp.inports,  key=lambda p: p.name)
            outs  = sorted(comp.outports, key=lambda p: p.name)
            n_in  = len(inps)
            n_out = len(outs)
            cols  = max(n_in, n_out, 1)


            in_cells = ''
            if n_in:
                for ip in inps:
                    in_cells += (
                        f'<TD PORT="{ip.name}" BORDER="1" COLOR="111111" '
                        'BGCOLOR="#2e2e2e" CELLPADDING="4" CELLSPACING="0">'
                        f'<FONT COLOR="white">{ip.name}</FONT>'
                        '</TD>'
                    )
                for _ in range(cols - n_in):
                    in_cells += (
                        '<TD BORDER="1" COLOR="111111" BGCOLOR="#2e2e2e" '
                        'CELLPADDING="4" CELLSPACING="0"></TD>'
                    )
            else:
                in_cells = (
                    f'<TD COLSPAN="{cols}" BORDER="1" COLOR="111111" '
                    'BGCOLOR="#2e2e2e" CELLPADDING="4" CELLSPACING="0"></TD>'
                )
            top_row = f'<TR>{in_cells}</TR>'


            mid_row = (
                f'<TR>'
                f'<TD COLSPAN="{cols}" BORDER="1" COLOR="111111" '
                'BGCOLOR="#444444" CELLPADDING="6" CELLSPACING="0">'
                f'<FONT COLOR="white"><B>{name}@{comp.__class__.__name__}</B></FONT>'
                '</TD>'
                '</TR>'
            )


            if n_out:
                base = cols // n_out
                rem  = cols - base * n_out
                out_cells = ''
                for i, op in enumerate(outs):
                    span = base + (1 if i == n_out - 1 and rem else 0)
                    out_cells += (
                        f'<TD PORT="{op.name}" COLSPAN="{span}" BORDER="1" COLOR="black" '
                        'BGCOLOR="#2e2e2e" CELLPADDING="4" CELLSPACING="0">'
                        f'<FONT COLOR="white">{op.name}</FONT>'
                        '</TD>'
                    )
            else:
                out_cells = (
                    f'<TD COLSPAN="{cols}" BORDER="1" COLOR="black" '
                    'BGCOLOR="#2e2e2e" CELLPADDING="4" CELLSPACING="0"></TD>'
                )
            bot_row = f'<TR>{out_cells}</TR>'

            html = (
                '<<TABLE BORDER="1" COLOR="black" CELLBORDER="0" CELLSPACING="0">\n'
                f'  {top_row}\n'
                f'  {mid_row}\n'
                f'  {bot_row}\n'
                '</TABLE>>'
            )
            dot.node(name, label=html)


        for outp, inp in self.edges:
            tail = f"{outp.component.name}:{outp.name}:s"
            head = f"{inp.component.name}:{inp.name}:n"
            dot.edge(tail, head)

        for name, comp in self.nodes.items():
            for ip in comp.inports:
                if ip.initialization_value is not None:
                    init = f'{name}.{ip.name}.init'
                    dot.node(
                        init,
                        label=f'< <FONT COLOR="white">{ip.initialization_value}</FONT> >',
                        shape='ellipse', style='filled',
                        fillcolor='#666666', fontcolor='white',
                        color='black'
                    )
                    dot.edge(init, f'{name}:{ip.name}')

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
            log.error(f"Exception in GraphWorker: {e}")
            traceback.print_exc()
        finally:
            loop.close()
