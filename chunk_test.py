from random import random
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

def test_limited_reads():
    source = BytesIO(b'0123456789')

    generator = chunk(source, limit=2)

    handle = next(generator)
    assert handle.read(2) == b'01'
    assert handle.read(2) == b'23'
    assert handle.read(2) == b'45'
    assert handle.read(2) == b'67'
    assert handle.read(2) == b'89'
    assert handle.read(2) == b''

def test_large_reads():
    source = BytesIO(b'0123456789')

    generator = chunk(source, limit=100)

    handle = next(generator)
    assert handle.read(100) == b'0123456789'
    assert handle.read(100) == b''

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

def test_fuzzy():
    bytes_to_read = int(2 ** (random() * 32767 / 1500))
    desired_size = int(2 ** (random() * 32767 / 1500))

    with open('/dev/urandom', 'rb') as handle:
        content = handle.read(bytes_to_read)
    source = BytesIO(content)
    output = BytesIO()

    for handle in chunk(source, limit=desired_size):
        data = _read_one_chunk(handle)
        output.write(data)

    result = output.getvalue()
    assert content == result, \
        'source has {source_length} bytes, output has {output_length} bytes'.format(
            source_length=len(content),
            output_length=len(result)
        )

def _read_one_chunk(handle):
    output = b''
    last_read_was_partial = False

    while True:
        request_size = int(2 ** (random() * 22))
        data = handle.read(request_size)
        read_size = len(data)
        output = output + data

        assert read_size <= request_size

        if data:
            assert not last_read_was_partial
        else:
            return output
        last_read_was_partial = read_size < request_size
