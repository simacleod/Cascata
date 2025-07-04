import asyncio
import multiprocess as multiprocessing
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cascata import Graph, inport, outport, component, persist


# simple helper similar to other tests
class DummyProcess:
    def __init__(self, target):
        self.target = target

    def start(self):
        self.target()

    def join(self):
        pass

    def terminate(self):
        pass


def run_graph(graph, workers=1):
    original = multiprocessing.Process
    multiprocessing.Process = DummyProcess
    try:
        graph.run(workers)
    finally:
        multiprocessing.Process = original


@component
@inport("count", default=5)
@outport("o")
async def Prod(count, o):
    for i in range(count):
        await o.send(i)


@component
@inport("items")
@persist("store", lambda lst: lst)
async def Collect(items, store):
    store.get().append(items)


@component
@inport("a")
@inport("b")
@persist("store", lambda lst: lst)
async def Pair(a, b, store):
    store.get().append((a, b))


def test_batch_all():
    result = []
    g = Graph()
    g.p = Prod
    g.c = Collect
    g.c.store < result
    g.p.o >>= g.c.items
    run_graph(g)
    assert result == [list(range(5))]


def test_batch_size_two():
    result = []
    g = Graph()
    g.p = Prod
    g.c = Collect
    g.c.store < result
    g.p.o >> 2 >= g.c.items
    run_graph(g)
    assert result == [[0, 1], [2, 3], [4]]


def test_group_with_batch():
    result = []
    g = Graph()
    g.p1 = Prod
    g.p2 = Prod
    g.p2.count < 4
    g.pair = Pair
    g.pair.store < result
    g.p1.o >> g.pair.a
    g.p2.o >> 2 >= g.pair.b
    g.pair.sync(g.pair.a, g.pair.b)
    run_graph(g)
    assert result == [(0, [0, 1]), (1, [2, 3])]

def test_group_sends_batch():
    result = []
    g = Graph()
    g.prods = Prod * 2
    g.prods.initialize('count', 1)
    g.collect = Collect
    g.collect.store < result
    g.prods.o >> 2 >= g.collect.items
    run_graph(g)
    assert result == [[0, 0]]
