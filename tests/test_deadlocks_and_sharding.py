import asyncio
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


@component
@inport('left')
@inport('right')
@outport('out')
async def Joiner(left, right, out):
    pass


@component
@inport('primary')
@inport('secondary')
@outport('out')
async def Merger(primary, secondary, out):
    pass


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


def test_shard_distributes_components_across_workers():
    g = Graph()
    g.source = Echo
    g.preprocess = Echo
    g.branch_left = Echo
    g.left_post = Echo
    g.left_tap = Echo
    g.branch_right = Echo
    g.right_mid = Echo
    g.right_post = Echo
    g.join_stage = Joiner
    g.after_join = Echo
    g.final_merge = Merger
    g.final_stage = Echo
    g.sink = Sink

    g.source.out >> g.preprocess.inp

    g.preprocess.out >> g.branch_left.inp
    g.preprocess.out >> g.branch_right.inp

    g.branch_left.out >> g.left_post.inp
    g.left_post.out >> g.join_stage.left
    g.left_post.out >> g.left_tap.inp

    g.branch_right.out >> g.right_mid.inp
    g.right_mid.out >> g.right_post.inp
    g.right_post.out >> g.join_stage.right

    g.left_tap.out >> g.final_merge.secondary

    g.join_stage.out >> g.after_join.inp
    g.after_join.out >> g.final_merge.primary

    g.final_merge.out >> g.final_stage.inp
    g.final_stage.out >> g.sink.inp

    num_workers = 4
    workers = g.shard(num_workers)

    assignments = {}
    for worker_idx, worker in enumerate(workers):
        for comp in worker.components:
            assignments[comp.name] = worker_idx

    assert len(assignments) == len(g.nodes)
    assert set(assignments.values()) == set(range(num_workers))
