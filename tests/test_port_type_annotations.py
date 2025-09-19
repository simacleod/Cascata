import pytest
from typing import Union
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

@component
@inport('inp')
async def IntOrStrSink(inp: Union[int, str]):
    pass

@component
@outport('out')
async def IntOrStrSource(out: Union[int, str]):
    await out.send(1)

@component
@inport('inp')
async def IntStrFloatSink(inp: Union[int, str, float]):
    pass


@component
@outport('out')
async def ListSource(out: list[int]):
    await out.send([1])


@component
@inport('inp')
async def ListSink(inp: list[int]):
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

def test_union_allows_subset_connection():
    g = Graph()
    g.src = IntSource
    g.sink = IntOrStrSink
    g.src.out >> g.sink.inp  # int is subset of Union[int, str]

def test_union_outport_rejected_for_single_type_inport():
    g = Graph()
    g.src = IntOrStrSource
    g.sink = IntSink
    with pytest.raises(TypeError):
        g.src.out >> g.sink.inp

def test_union_outport_to_union_superset_connection():
    g = Graph()
    g.src = IntOrStrSource
    g.sink = IntStrFloatSink
    g.src.out >> g.sink.inp  # Union[int, str] subset of Union[int, str, float]


def test_parameterized_generic_annotations_connection():
    g = Graph()
    g.src = ListSource
    g.sink = ListSink
    g.src.out >> g.sink.inp  # should not raise when using parameterized generics
