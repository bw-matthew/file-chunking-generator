from io import BytesIO
from chunk import chunk, LimitedFile
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

def test_medium_input_two_output():
    source = BytesIO(b'0123456789')

    generator = chunk(source, limit=6, delimiter=b'5')

    assert next(generator).read() == b'012345'
    assert next(generator).read() == b'6789'

    with raises(StopIteration):
        next(generator)

def test_medium_input_no_delimiter():
    source = BytesIO(b'0123456789')

    generator = chunk(source, limit=6, delimiter=b'9')

    with raises(ValueError):
        next(generator)

def test_small_input_limited_file():
    source = BytesIO(b'0123456789')

    handle = LimitedFile(source)
    content = handle.read()
    handle.close()

    assert content == b'0123456789'

def test_medium_input_limited_file():
    source = BytesIO(b'0123456789')

    handle = LimitedFile(source, limit=6)
    content = handle.read()
    handle.close()

    assert content == b'012345'
