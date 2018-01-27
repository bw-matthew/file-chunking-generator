from io import BytesIO
from chunk import chunk
from pytest import raises

def test_no_input_no_output():
    source = BytesIO(b'')

    generator = chunk(source)

    with raises(StopIteration):
        next(generator)

def test_small_input_one_output():
    source = BytesIO(b'0123456789')

    generator = chunk(source)

    handle = next(generator)
    assert handle.read() == b'0123456789'

    with raises(StopIteration):
        next(generator)
