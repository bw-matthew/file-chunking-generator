""" Chunks a file into a bunch of files based on a delimiter and a size limit """

from io import BytesIO, RawIOBase

DEFAULT_LIMIT = 1024 * 1024
DEFAULT_BUFFER_SIZE = 4 * 1024
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

class LimitedFile(RawIOBase):

    def __init__(self, source, limit=DEFAULT_LIMIT, buffer_size=DEFAULT_BUFFER_SIZE, remainder=b''):
        super().__init__()
        self.source = source
        self.limit = limit
        self.buffer = bytearray(buffer_size)
        self.remainder = remainder

    def readinto(self, buffer):
        if self.limit < 1:
            return 0

        buffer_size = min(len(buffer), self.limit)
        remainder_size = len(self.remainder)

        if buffer_size <= remainder_size:
            buffer[:], self.remainder = self.remainder[:buffer_size], self.remainder[buffer_size:]
            return buffer_size

        read_size = self.source.readinto(self.buffer)
        if read_size + remainder_size == 0:
            return 0

        data = self.remainder + self.buffer[:read_size]
        buffer[:], self.remainder = data[:buffer_size], data[buffer_size:]

        return_size = min(buffer_size, read_size)
        self.limit = self.limit - return_size
        return return_size