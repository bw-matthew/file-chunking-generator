""" Chunks a file into a bunch of files based on a delimiter and a size limit """

from io import BytesIO

DEFAULT_LIMIT = 1024 * 1024
DEFAULT_DELIMITER = b'\n'

def chunk(source, limit=DEFAULT_LIMIT, delimiter=DEFAULT_DELIMITER):
    remainder = b''
    while True:
        size = limit - len(remainder)
        buffer = source.read(size)
        if not buffer:
            return

        if len(buffer) < size:
            yield BytesIO(remainder + buffer)
            continue

        data, delim, remainder = (remainder + buffer).rpartition(delimiter)
        if not delim:
            raise ValueError('No delimiter found within chunk')

        yield BytesIO(data + delim)
