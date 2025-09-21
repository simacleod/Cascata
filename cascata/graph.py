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
        for node, data in new.networkx_graph.nodes(data=True):
            self.networkx_graph.add_node(node, **data)
        for u, v in new.networkx_graph.edges:
            self.networkx_graph.add_edge(u, v)
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

        # Clone component groups
        new.group_handles = {}
        for alias, group in self.group_handles.items():
            new_alias = f'{gname}_{alias}'
            new_group = ComponentGroup(group.comp_cls, group.count)
            new_group.name = new_alias
            new_group.graph = new
            for i in range(group.count):
                comp_name = f'{new_alias}_{i}'
                comp = new.nodes[comp_name]
                new_group.group.append(comp)
                comp.group_name = new_alias
            new.group_handles[new_alias] = new_group
            new.__dict__[new_alias] = new_group
            new._local_namespace[new_alias] = new_group

        new.edges = []
        new.networkx_graph = nx.DiGraph()
        for outp, inp in self.edges:
            cloned_out_comp = comp_map[outp.component]
            cloned_in_comp  = comp_map[inp.component]
            new_outp = getattr(cloned_out_comp, outp.name)
            new_inp  = getattr(cloned_in_comp,  inp.name)
            saved_init = new_inp.initialization_value
            new_outp.connect(new_inp)
            if isinstance(saved_init, OutputPort):
                mapped_comp = comp_map.get(saved_init.component)
                if mapped_comp is not None:
                    saved_init = getattr(mapped_comp, saved_init.name)
            new_inp.initialization_value = saved_init

        new._exports = {}
        for export_name, port in self._exports.items():
            comp = port.component
            if isinstance(comp, ComponentGroup):
                alias = f'{gname}_{comp.name}'
                cloned_group = new.group_handles[alias]
                cloned_port = getattr(cloned_group, port.name)
            else:
                cloned_comp = comp_map[comp]
                cloned_port = getattr(cloned_comp, port.name)
            new._exports[export_name] = cloned_port

        return new


    def to_json(self):
        def encode_init(val):
            if isinstance(val, OutputPort):
                return {"component": val.component.name, "port": val.name}
            return val

        nodes = {}
        for name, comp in self.nodes.items():
            # Skip individual members of ComponentGroup; they are captured via
            # a single entry in ``group_handles`` below.
            if comp.group_name is not None or isinstance(comp, GroupConnector):
                continue

            info = {
                "module": comp.__class__.__module__,
                "class": comp.__class__.__name__,
                "group": comp.group_name,
                "inports": {
                    ip.name: {
                        "capacity": getattr(ip.channel, "capacity", None),
                        "initial": encode_init(ip.initialization_value),
                        "batch_size": ip.batch_size,
                    }
                    for ip in comp.inports
                },
            }
            nodes[name] = info

        edgeset = set()
        for o, i in self.edges:
            if isinstance(o.component, GroupConnector) or isinstance(i.component, GroupConnector):
                continue
            src = o.component.group_name or o.component.name
            dst = i.component.group_name or i.component.name
            edgeset.add((src, o.name, dst, i.name))

        for conn in (c for c in self.nodes.values() if isinstance(c, GroupConnector)):
            ups = [o for o, inp in self.edges if inp.component is conn]
            downs = [inp for outp, inp in self.edges if outp.component is conn]
            if not ups or not downs:
                continue
            src = ups[0].component.group_name or ups[0].component.name
            dst = downs[0].component.group_name or downs[0].component.name
            edgeset.add((src, ups[0].name, dst, downs[0].name))

        edges = [list(e) for e in sorted(edgeset)]

        groups = {}
        for alias, grp in self.group_handles.items():
            first = grp.group[0]
            groups[alias] = {
                "module": grp.comp_cls.__module__,
                "class": grp.comp_cls.__name__,
                "count": grp.count,
                "inports": {
                    ip.name: {
                        "capacity": getattr(ip.channel, "capacity", None),
                        "initial": encode_init(ip.initialization_value),
                        "batch_size": ip.batch_size,
                    }
                    for ip in first.inports
                },
            }

        exports = {}
        for name, port in self._exports.items():
            if isinstance(port.component, ComponentGroup):
                exports[name] = {"group": port.component.name, "port": port.name}
            else:
                exports[name] = {
                    "component": port.component.name,
                    "port": port.name,
                }

        subgraphs = {
            alias: sg.to_json()
            for alias, sg in self.__dict__.items()
            if isinstance(sg, Graph) and sg.is_subgraph
        }

        return {
            "nodes": nodes,
            "edges": edges,
            "group_handles": groups,
            "exports": exports,
            "subgraphs": subgraphs,
        }

    @classmethod
    def from_json(cls, data):
        import importlib

        def load_cls(mod, name):
            module = importlib.import_module(mod)
            return getattr(module, name)

        g = cls()

        # subgraphs first
        for alias, subdata in data.get("subgraphs", {}).items():
            sub = cls.from_json(subdata)
            sub.is_subgraph = True
            sub.parent = g
            g.nodes.update(sub.nodes)
            g.edges.extend(sub.edges)
            for node, data2 in sub.networkx_graph.nodes(data=True):
                g.networkx_graph.add_node(node, **data2)
            for u, v in sub.networkx_graph.edges:
                g.networkx_graph.add_edge(u, v)
            g.__dict__[alias] = sub
            g._local_namespace[alias] = sub

        # groups
        for alias, info in data.get("group_handles", {}).items():
            comp_cls = load_cls(info["module"], info["class"])
            group = ComponentGroup(comp_cls, info["count"])
            g._add_component_group(alias, group)

            for comp in group.group:
                for ip_name, ip_info in info.get("inports", {}).items():
                    ip = getattr(comp, ip_name)
                    val = ip_info.get("initial")
                    if isinstance(val, dict) and "component" in val:
                        src = g.nodes[val["component"]]
                        val = getattr(src, val["port"])
                    ip.initialization_value = val
                    ip.batch_size = ip_info.get("batch_size")
                    cap = ip_info.get("capacity")
                    if cap is not None:
                        ip.channel.capacity = cap

        # components
        for name, info in data.get("nodes", {}).items():
            if name in g.nodes:
                comp = g.nodes[name]
            else:
                comp_cls = load_cls(info["module"], info["class"])
                comp = comp_cls(name)
                g.node(name, comp)

            for ip_name, ip_info in info.get("inports", {}).items():
                ip = getattr(comp, ip_name)
                val = ip_info.get("initial")
                if isinstance(val, dict) and "component" in val:
                    src = g.nodes[val["component"]]
                    val = getattr(src, val["port"])
                ip.initialization_value = val
                ip.batch_size = ip_info.get("batch_size")
                cap = ip_info.get("capacity")
                if cap is not None:
                    ip.channel.capacity = cap

        for out_c, out_p, in_c, in_p in data.get("edges", []):
            if out_c in g.group_handles:
                src = getattr(g.group_handles[out_c], out_p)
            else:
                src = getattr(g.nodes[out_c], out_p)
            if in_c in g.group_handles:
                dst = getattr(g.group_handles[in_c], in_p)
            else:
                dst = getattr(g.nodes[in_c], in_p)
            src >> dst

        for name, info in data.get("exports", {}).items():
            if "group" in info:
                port = getattr(g.group_handles[info["group"]], info["port"])
            else:
                port = getattr(g.nodes[info["component"]], info["port"])
            g._exports[name] = port
        
        serialized_names = set(data.get("nodes", {}).keys())
        for alias, info in data.get("group_handles", {}).items():
            serialized_names.update(f"{alias}_{i}" for i in range(info.get("count", 0)))
        for subdata in data.get("subgraphs", {}).values():
            serialized_names.update(subdata.get("nodes", {}).keys())
            for alias, info in subdata.get("group_handles", {}).items():
                serialized_names.update(
                    f"{alias}_{i}" for i in range(info.get("count", 0))
                )
        extra = [
            n
            for n in list(g.nodes.keys())
            if n not in serialized_names and not isinstance(g.nodes[n], GroupConnector)
        ]
        for name in extra:
            comp = g.nodes.pop(name)
            if g.networkx_graph.has_node(comp):
                g.networkx_graph.remove_node(comp)

        return g

    def _break_cycles(self):
        """Return an acyclic copy of the scheduling graph.

        The original ``networkx_graph`` remains unchanged; the returned copy
        has enough edges removed to make it acyclic for topological sorting.
        """
        acyclic = self.networkx_graph.copy()
        while True:
            try:
                cycle = nx.find_cycle(acyclic, orientation="original")
            except nx.NetworkXNoCycle:
                break
            # Removing an edge to break the cycle
            u, v = cycle[0][:2]
            acyclic.remove_edge(u, v)
        return acyclic

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
            nodes = list(scc)
            if len(nodes) > 1 or any(self.networkx_graph.has_edge(n, n) for n in nodes):
                # ensure at least one component in the cycle has an inport.initialization_value
                seeded = False
                resolved = []
                for node in nodes:
                    if isinstance(node, Component):
                        comp = node
                    elif node in self.nodes:
                        comp = self.nodes[node]
                    else:
                        comp = None
                    resolved.append((node, comp))
                    if comp:
                        for ip in comp.inports:
                            if ip.initialization_value is not None:
                                seeded = True
                                break
                    if seeded:
                        break
                if seeded:
                    continue
                component_names = sorted(
                    (
                        comp.name
                        if comp is not None
                        else getattr(node, "name", str(node))
                    )
                    for node, comp in resolved
                )
                issues.append(
                    f"Cycle deadlock: components {component_names} form a cycle with no default seed."
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
        G = self._break_cycles()
    
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

        # Ensure every component has an assignment, even if it was not part of the
        # DAG (for example, connectors or isolated nodes that never appeared in
        # the networkx graph).
        for comp in self.nodes.values():
            if comp not in color_map:
                color_map[comp] = rr
                rr = (rr + 1) % num_workers
    
        # 5) Instantiate workers and assign components
        workers = [GraphWorker() for _ in range(num_workers)]
        for comp_name, comp in self.nodes.items():
            worker_index = color_map.get(comp, 0)
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
            log.warning("\nGraph execution cancelled by user.")
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
            log.warning('to_dot requires graphviz:\n    pip install graphviz')
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
