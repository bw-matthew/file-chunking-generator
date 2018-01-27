from io import BytesIO
from chunk import chunk
from pytest import raises

def test_no_input_no_output():
    source = BytesIO(b'')

    generator = chunk(source)

    with raises(StopIteration):
        next(generator)
