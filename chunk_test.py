from io import BytesIO
from chunk import chunk, LimitedReader
from pytest import raises

def test_no_input_no_output():
    source = BytesIO(b'')

    generator = chunk(source)

    handle = next(generator)
    assert handle.read() == b''

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

    handle = next(generator)
    assert handle.read() == b'0123456789'

    with raises(StopIteration):
        next(generator)

def test_limited_reader_with_small_input():
    source = BytesIO(b'0123456789')

    handle = LimitedReader(source, limit=20, delimiter=b'5')
    content = handle.read()
    handle.close()

    assert content == b'0123456789'

def test_limited_reader_with_medium_input():
    source = BytesIO(b'0123456789')

    handle = LimitedReader(source, limit=6, delimiter=b'5')
    content = handle.read()
    handle.close()

    assert content == b'012345'
    assert handle.remainder == b'6789'

def test_limited_reader_with_medium_input_and_trailing_delimiter():
    source = BytesIO(b'0123456789')

    handle = LimitedReader(source, limit=6, delimiter=b'9')
    content = handle.read()
    handle.close()

    assert content == b'0123456789'
