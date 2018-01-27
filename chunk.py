""" Chunks a file into a bunch of files based on a delimiter and a size limit """

from io import BytesIO

DEFAULT_LIMIT = 1024 * 1024
DEFAULT_DELIMITER = b'\n'

def chunk(source, limit=DEFAULT_LIMIT, delimiter=DEFAULT_DELIMITER):
    remainder = b''
    while True:
        buffer = source.read(limit - len(remainder))
        if not buffer:
            return

        data, delimiter, remainder = buffer.rpartition(remainder + delimiter)
        if not delimiter:
            raise ValueError('No delimiter found within chunk')

        yield BytesIO(data + delimiter)
