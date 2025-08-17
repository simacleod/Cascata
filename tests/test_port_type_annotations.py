import pytest
from cascata import Graph, component, inport, outport

@component
@outport('out')
async def IntSource(out: int):
    await out.send(1)

@component
@inport('inp')
async def IntSink(inp: int):
    pass

@component
@inport('inp')
async def StrSink(inp: str):
    pass

@component
@inport('inp')
async def AnySink(inp):
    pass

def test_type_mismatch_connection():
    g = Graph()
    g.src = IntSource
    g.sink = StrSink
    with pytest.raises(TypeError):
        g.src.out >> g.sink.inp

def test_type_match_connection():
    g = Graph()
    g.src = IntSource
    g.sink = IntSink
    g.src.out >> g.sink.inp  # no error expected

def test_missing_annotation_connection():
    g = Graph()
    g.src = IntSource
    g.sink = AnySink
    g.src.out >> g.sink.inp  # allowed without annotations
