import os
import sys
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from cascata import Graph, component, inport, outport



@component
@outport('o')
async def Prod(o):
    await o.send(1)
Prod.__module__ = __name__

@component
@inport('i', capacity=5, default=7)
async def Cons(i):
    pass
Cons.__module__ = __name__


def build_graph():
    g = Graph()
    g.p = Prod
    g.consumers = Cons * 2
    g.consumers.initialize('i', 7)
    g.p.o >> g.consumers.i
    for c in g.consumers.group:
        c.i.batch_size = 2
    g.extra = Cons
    g.extra.i.initialize(42)
    return g


def test_graph_to_from_json_roundtrip():
    g1 = build_graph()
    data = g1.to_json()
    # ensure json serializable
    json.dumps(data)

    # grouped components should serialize once with count
    assert 'consumers_0' not in data['nodes']
    assert 'consumers' in data['group_handles']
    ginfo = data['group_handles']['consumers']
    assert ginfo['count'] == 2
    assert ginfo['inports']['i']['capacity'] == 5
    assert ginfo['inports']['i']['batch_size'] == 2


    g2 = Graph.from_json(data)

    assert set(g1.nodes.keys()) == set(g2.nodes.keys())
    assert set(g1.group_handles.keys()) == set(g2.group_handles.keys())

    # check port properties preserved
    c0 = g2.consumers.group[0]
    assert c0.i.channel.capacity == 5
    assert c0.i.batch_size == 2
    assert g2.extra.i.initialization_value == 42

    edges1 = sorted([(o.component.name,o.name,i.component.name,i.name) for o,i in g1.edges])
    edges2 = sorted([(o.component.name,o.name,i.component.name,i.name) for o,i in g2.edges])
    assert edges1 == edges2

    # round trip serialization should be stable
    assert g1.to_json() == Graph.from_json(g1.to_json()).to_json()

    g1.run()
    g2.run()


def test_subgraph_group_serialization():
    sg = Graph()
    sg.prods = Prod * 3
    sg.consumers = Cons * 3
    for c in sg.consumers.group:
        c.i.batch_size = 4
    sg.prods.o >> sg.consumers.i

    data = sg.to_json()
    assert 'consumers_0' not in data['nodes']
    assert data['group_handles']['consumers']['count'] == 3
    assert data['group_handles']['consumers']['inports']['i']['batch_size'] == 4

    sg2 = Graph.from_json(data)
    assert set(sg.nodes.keys()) == set(sg2.nodes.keys())
    c0 = sg2.consumers.group[0]
    assert c0.i.batch_size == 4

    sg.run()
    sg2.run()
