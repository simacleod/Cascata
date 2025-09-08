from cascata import inport, outport, component


@component
@inport('x')
@outport('y')
async def echo(x, y):
    await y.send(x)


def test_component_test_basic():
    result = echo.test(x=5)
    assert result == {'y': [5]}


@component
@inport('a')
@outport('b')
@outport('c')
async def multi(a, b, c):
    await b.send(a)
    await c.send(a * 2)


def test_component_test_multiple_outputs():
    result = multi.test(a=3)
    assert result == {'b': [3], 'c': [6]}
