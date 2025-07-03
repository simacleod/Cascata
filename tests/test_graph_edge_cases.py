import asyncio
import importlib
import sys
import multiprocess as multiprocessing
import networkx as nx

import pytest
from cascata import component, inport, outport, Graph
from cascata.port import PersistentValue
from cascata.log import log

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

def run_graph(graph, process_cls=DummyProcess):
    original = multiprocessing.Process
    multiprocessing.Process = process_cls
    try:
        graph.run(1)
    finally:
        multiprocessing.Process = original

@component
@outport('o')
async def Prod(o):
    await o.send(1)

@component
@inport('i')
async def Cons(i):
    pass

@component
@inport('a')
@inport('b')
async def TwoInputs(a, b):
    pass

@component
@inport('i', default=1)
@outport('o')
async def Seeded(i, o):
    await o.send(i)

@component
@inport('i')
@outport('o')
async def Pipe(i, o):
    await o.send(i)

@component
@inport('i')
@outport('o1')
@outport('o2')
async def ManyOut(i, o1, o2):
    await o1.send(i)
    await o2.send(i)

def test_persistent_value_extra_paths():
    pv = PersistentValue(lambda a, b=0: [a, b])
    pv < (1, 2)
    assert pv.get() == [1, 2]
    pv < {'a': 5, 'b': 3}
    assert pv.get() == [5, 3]

def test_sync_and_listener_no_runner():
    c = TwoInputs('c')
    c.sync(c.b)
    async def send_and_listen():
        async with c.a.channel.open():
            await c.a.channel.put(42)
        await c._group_listener((c.a,), False)
        assert c._vals[c.a] == 42
    asyncio.run(send_and_listen())


def test_logging_emit_error(monkeypatch):
    handler = log.handlers[0]
    called = []
    monkeypatch.setattr(handler, 'handleError', lambda record: called.append(True))
    class BadStream:
        def write(self, msg):
            raise ValueError
    monkeypatch.setattr(handler, 'stream', BadStream())
    log.info('boom')
    assert called


def test_run_initialization_from_output():
    @component
    @outport('o')
    async def Src(o):
        await o.send(7)

    @component
    @inport('i')
    async def Dest(i):
        pass

    g = Graph()
    g.s = Src
    g.d = Dest
    g.s.o >> g.d.i
    g.d.i.initialization_value = g.s.o

    async def run_pair():
        async with asyncio.TaskGroup() as tg:
            tg.create_task(g.s.run())
            tg.create_task(g.d.run())

    asyncio.run(run_pair())


def test_run_adds_ungrouped_ports():
    @component
    @inport('a')
    @inport('b')
    async def Comp(a, b):
        pass

    c = Comp('x')
    c.clk(c.b)

    async def feed():
        async with c.a.channel.open():
            await c.a.channel.put(1)

    async def run_all():
        async with asyncio.TaskGroup() as tg:
            tg.create_task(feed())
            async with c.b.channel.open():
                await c.b.channel.put(2)
            tg.create_task(c.run())

    asyncio.run(run_all())


def test_to_dot_padding():
    g = Graph()
    g.m = ManyOut
    from cascata.graph import Digraph
    dot = g.to_dot()
    if Digraph:
        assert 'digraph' in dot.source
    else:
        assert dot is None


def test_graph_setattr_instance():
    g = Graph()
    inst = Prod('inst')
    g.inst = inst
    assert g.inst is inst and 'inst' in g.nodes


def test_find_root_graph_branch():
    sub = Graph()
    sub.c = Cons
    sub.export(sub.c.i, 'inp')
    g = Graph()
    g.sub = sub
    g.p = Prod
    g.p.o >> g.sub.inp
    assert (g.p.o, g.sub.inp) in g.edges


def test_break_cycles_true(monkeypatch):
    g = Graph()
    g.a = Pipe
    g.b = Pipe
    g.a.o >> g.b.i
    g.b.o >> g.a.i
    monkeypatch.setattr(nx, 'find_cycle', lambda G, orientation='original': [(g.a, g.b)])
    assert g._break_cycles() is True
    assert (g.a, g.b) not in g.networkx_graph.edges


def test_check_deadlocks_seeded_cycle():
    g = Graph()
    g.a = Pipe
    g.b = Pipe
    g.a.o >> g.b.i
    g.b.o >> g.a.i
    g.a.i.initialize(1)
    g.nodes[g.a] = g.a
    g.nodes[g.b] = g.b
    g.check_deadlocks()


def test_shard_branches():
    g1 = Graph()
    g1.p1 = Prod
    g1.p2 = Prod
    g1.cons = Cons * 2
    g1.p1.o >> g1.cons.group[0].i
    g1.p2.o >> g1.cons.group[1].i
    calls = iter([True, False])
    def fake_break():
        return next(calls)
    g1._break_cycles = fake_break
    g1.shard(2)

    g2 = Graph()
    g2.p = Prod
    g2.cons = Cons * 2
    g2.p.o >> g2.cons.group[0].i
    g2.p.o >> g2.cons.group[1].i
    g2.shard(3)


def test_join_keyboard_interrupt(monkeypatch, caplog):
    class KIProcess(DummyProcess):
        def join(self):
            raise KeyboardInterrupt
    caplog.set_level('WARNING')
    g = Graph()
    g.p = Prod
    handler = log.handlers[0]
    from io import StringIO
    stream = StringIO()
    monkeypatch.setattr(handler, 'stream', stream)
    run_graph(g, process_cls=KIProcess)
    assert 'Graph execution cancelled' in stream.getvalue()


def test_reload_without_graphviz(monkeypatch):
    import cascata.graph as gmod
    saved = sys.modules.pop('graphviz', None)
    sys.modules['graphviz'] = None
    importlib.reload(gmod)
    assert gmod.Digraph is False
    gmod.Graph().to_dot()
    if saved is not None:
        sys.modules['graphviz'] = saved
    else:
        sys.modules.pop('graphviz', None)
    importlib.reload(gmod)


def test_start_default_workers(monkeypatch):
    g = Graph()
    g.p = Prod
    monkeypatch.setattr(multiprocessing, 'cpu_count', lambda: 1)
    class Proc(DummyProcess):
        pass
    original = multiprocessing.Process
    multiprocessing.Process = Proc
    try:
        g.start()
        g.join()
    finally:
        multiprocessing.Process = original
