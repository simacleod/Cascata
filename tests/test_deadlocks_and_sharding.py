import asyncio
import multiprocess as multiprocessing
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from cascata import Graph, inport, outport, component
from cascata.graph import DeadlockError, Digraph
from cascata.component import GroupConnector

@component
@inport('inp')
@outport('out')
async def Echo(inp, out):
    await out.send(inp)

@component
@inport('inp')
async def Sink(inp):
    pass

# Utility similar to run_graph from other tests
class DummyProcess:
    def __init__(self, target):
        self.target = target
    def start(self):
        self.target()
    def join(self):
        pass
    def terminate(self):
        pass

def run_graph(graph, num_workers=1):
    original = multiprocessing.Process
    multiprocessing.Process = DummyProcess
    try:
        graph.run(num_workers)
    finally:
        multiprocessing.Process = original


def test_component_repr():
    comp = Echo('e')
    text = repr(comp)
    # representation should include port names and component class name
    assert 'inp' in text and 'out' in text and 'Echo' in text


def test_deadlock_cycle():
    g = Graph()
    g.a = Echo
    g.b = Echo
    g.a.out >> g.b.inp
    g.b.out >> g.a.inp
    import networkx as nx
    g.networkx_graph = nx.DiGraph()
    for outp, inp in g.edges:
        g.networkx_graph.add_edge(outp.component.name, inp.component.name)
    with pytest.raises(DeadlockError) as exc:
        g.check_deadlocks()
    assert 'Cycle deadlock' in str(exc.value)


def test_deadlock_orphan_port():
    g = Graph()
    g.sink = Sink
    with pytest.raises(DeadlockError) as exc:
        g.check_deadlocks()
    assert "Orphan port" in str(exc.value)


def test_deadlock_trigger():
    g = Graph()
    g.e = Echo
    g.e.clk(g.e.inp)
    with pytest.raises(DeadlockError) as exc:
        g.check_deadlocks()
    assert 'Trigger deadlock' in str(exc.value)


def test_deadlock_unconnected_export():
    g = Graph()
    g.e = Echo
    g.export(g.e.inp, 'ext')
    with pytest.raises(DeadlockError) as exc:
        g.check_deadlocks()
    assert 'Export deadlock' in str(exc.value)


def test_to_dot_without_graphviz():
    g = Graph()
    result = g.to_dot()
    if Digraph:
        assert 'digraph' in result.source
    else:
        assert result is None


def test_shard_and_group_connector():
    g = Graph()
    g.prods = Echo * 2
    g.sink = Sink
    g.prods.out >> g.sink.inp
    workers = g.shard(2)
    # Connector node should exist
    assert any(isinstance(c, GroupConnector) for c in g.nodes.values())
    # All components assigned across two workers
    total = sum(len(w.components) for w in workers)
    assert total == len(g.nodes)
