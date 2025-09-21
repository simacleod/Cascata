import asyncio
import multiprocessing
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from cascata import Graph, inport, outport, component, persist

# Define simple components for tests
@component
@inport('count', default=5)
@outport('out')
async def Producer(count, out):
    for i in range(count):
        await out.send(i)

@component
@inport('inp')
@outport('out')
async def Forward(inp, out):
    await out.send(inp)

@component
@inport('item')
@persist('store', lambda lst: lst)
async def Collector(item, store):
    store.get().append(item)


def test_component_creation():
    p = Producer('prod')
    c = Collector('c')
    assert {ip.name for ip in p.inports} == {'count'}
    assert {op.name for op in p.outports} == {'out'}
    assert 'store' in c.ports


def test_graph_creation_and_run():
    mgr = multiprocessing.Manager()
    result = mgr.list()

    g = Graph()
    g.producer = Producer
    g.forward = Forward
    g.collector = Collector

    g.collector.store < result
    g.producer.out >> g.forward.inp
    g.forward.out >> g.collector.item

    g.run(1)

    assert list(result) == list(range(5))


def test_graph_with_subgraph():
    mgr = multiprocessing.Manager()
    result = mgr.list()

    sub = Graph()
    sub.p = Producer
    sub.export(sub.p.out, 'out')

    g = Graph()
    g.sub = sub
    g.collector = Collector
    g.collector.store < result

    g.sub.out >> g.collector.item

    g.run(1)

    assert list(result) == list(range(5))


def test_grouped_components():
    mgr = multiprocessing.Manager()
    result = mgr.list()

    g = Graph()
    g.prods = Producer * 2
    g.colls = Collector * 2
    g.colls.store < result

    g.prods.out >> g.colls.item
    g.prods.initialize('count', 2)

    g.run(1)

    assert sorted(result) == [0, 0, 1, 1]


def test_subgraph_and_grouped_components():
    mgr = multiprocessing.Manager()
    result = mgr.list()

    sub = Graph()
    sub.p = Producer
    sub.export(sub.p.out, 'out')

    g = Graph()
    g.sub = sub
    g.colls = Collector * 2
    g.colls.store < result

    g.sub.out >> g.colls.item
    g.colls.store < result

    g.run(1)

    assert sorted(result) == list(range(5))


def test_subgraph_with_group_inside():
    mgr = multiprocessing.Manager()
    result = mgr.list()

    sub = Graph()
    sub.prods = Producer * 2
    sub.merger = Forward
    sub.prods.out >> sub.merger.inp
    sub.export(sub.merger.out, 'out')

    sub.prods.initialize('count', 2)

    g = Graph()
    g.sub = sub
    g.collector = Collector
    g.collector.store < result

    g.sub.out >> g.collector.item
    g.producer = Producer
    g.producer.count < 0  # avoid extra output

    g.run(1)

    assert sorted(result) == [0, 0, 1, 1]


def test_subgraph_exporting_group_port():
    mgr = multiprocessing.Manager()
    result = mgr.list()

    sub = Graph()
    sub.prods = Producer * 2
    sub.export(sub.prods.out, 'out')
    sub.prods.initialize('count', 2)

    g = Graph()
    g.sub = sub
    g.colls = Collector * 2
    g.colls.store < result

    g.sub.out >> g.colls.item

    g.run(1)

    assert sorted(result) == [0, 0, 1, 1]
