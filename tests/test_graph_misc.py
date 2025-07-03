import asyncio
import multiprocess as multiprocessing
import os
import sys
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
from cascata import Graph, inport, outport, component, persist
from cascata.graph import GraphWorker, DeadlockError, Digraph
from cascata.port import InputPort, OutputPort, PortHandler, PersistentValue
from cascata.log import log, _get_component_color

# Utility patch similar to other tests
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
    orig = multiprocessing.Process
    multiprocessing.Process = DummyProcess
    try:
        graph.run(workers)
    finally:
        multiprocessing.Process = orig

# Simple components
@component
@outport('o')
async def Prod(o):
    await o.send(1)

@component
@inport('i')
async def Cons(i):
    pass

@component
@inport('i')
@outport('o')
async def Pipe(i, o):
    await o.send(i)

@component
@inport('i1')
@inport('i2')
@outport('o')
async def Combiner(i1, i2, o):
    await o.send((i1, i2))

@component
async def Fail():
    raise RuntimeError('boom')

@component
async def Logger():
    log.info('hi')


def test_get_component_color():
    # color code should start with escape sequence
    c = _get_component_color('MyComp')
    assert c.startswith('\x1b[38;5;')

def test_persistent_value_lt_tuple():
    pv = PersistentValue(lambda x,y: x+y)
    pv < ((1,2), {})
    assert pv.get() == 3

def test_component_mul_invalid():
    with pytest.raises(TypeError):
        Prod * 2.5


def test_component_copy_persist():
    @component
    @persist('p', list)
    async def P(p):
        pass
    inst = P('x')
    inst.p < [1,2]
    inst.p.get()
    copied = inst.copy()
    assert copied.p.get() == [1,2]


def test_group_repr_and_port_handler():
    g = Graph()
    g.group = Prod * 2
    group = g.group
    text = repr(group)
    assert str(group.count) in text
    # access port via PortHandler
    ph = group.o
    assert isinstance(ph, PortHandler)


def test_port_lt_and_repr():
    g = Graph()
    g.p = Pipe
    g.c = Cons
    ip = g.c.i
    op = g.p.o
    repr(ip)
    repr(op)
    ip < 1
    op >> ip


def test_graph_attribute_error_and_export():
    g = Graph()
    g.p = Prod
    with pytest.raises(AttributeError):
        g.missing
    with pytest.raises(ValueError):
        g.export(g.p.o, 'x')
        g.export(g.p.o, 'x')


def test_break_cycles_and_shard():
    g = Graph()
    g.a = Pipe
    g.b = Pipe
    g.a.o >> g.b.i
    g.b.o >> g.a.i
    with pytest.raises(TypeError):
        g._break_cycles()
    g2 = Graph()
    g2.p1 = Pipe
    g2.p2 = Pipe
    g2.p1.o >> g2.p2.i
    workers = g2.shard(2)
    assert len(workers) == 2


def test_graph_to_dot_with_graphviz():
    g = Graph()
    g.p = Prod
    g.c = Cons
    g.p.o >> g.c.i
    dot = g.to_dot()
    if Digraph:
        assert 'digraph' in dot.source
    else:
        assert dot is None

def test_to_dot_initial_values():
    @component
    @outport('o')
    async def Src(o):
        await o.send(5)

    @component
    @inport('inp')
    async def Tgt(inp):
        pass

    g = Graph()
    g.src = Src
    g.tgt = Tgt
    g.tgt.inp < 1
    dot = g.to_dot()
    if Digraph:
        assert 'init' in dot.source
    else:
        assert dot is None


def test_logging_context_and_error(monkeypatch):
    g = Graph()
    g.l = Logger
    from io import StringIO
    stream = StringIO()
    handler = log.handlers[0]
    monkeypatch.setattr(handler, 'stream', stream)
    run_graph(g)
    assert 'Logger' in stream.getvalue()

    class BadStream(list):
        def write(self, msg):
            raise ValueError


def test_graphworker_exception_logging(capfd):
    from io import StringIO
    stream = StringIO()
    handler = log.handlers[0]
    old_stream = handler.stream
    handler.stream = stream
    g = Graph()
    g.f = Fail
    run_graph(g)
    handler.stream = old_stream
    assert 'Exception in GraphWorker' in stream.getvalue()

