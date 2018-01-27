""" Chunks a file into a bunch of files based on a delimiter and a size limit """

from io import BytesIO

DEFAULT_LIMIT = 1024 * 1024
DEFAULT_DELIMITER = b'\n'

def chunk(source, limit=DEFAULT_LIMIT, delimiter=DEFAULT_DELIMITER):
    remainder = b''
    while True:
        buffer = _read(source, remainder, limit)
        if not buffer:
            return

        if _is_within_limit(buffer, limit):
            yield BytesIO(buffer)
        else:
            data, remainder = _partition(buffer, delimiter)
            yield BytesIO(data)

def _read(source, remainder, limit):
    size = limit - len(remainder)
    buffer = source.read(size)
    return remainder + buffer

def _is_within_limit(buffer, limit):
    return len(buffer) < limit

def _partition(buffer, delimiter):
    data, delim, remainder = buffer.rpartition(delimiter)
    if not delim:
        raise ValueError('No delimiter found within chunk')
    return data + delim, remainder
