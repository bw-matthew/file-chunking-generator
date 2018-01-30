""" Chunks a file into a bunch of files based on a delimiter and a size limit """

from io import BytesIO, RawIOBase

DEFAULT_LIMIT = 1024 * 1024
DEFAULT_BUFFER_SIZE = 4 * 1024
DEFAULT_DELIMITER = b'\n'

def chunk(
        source,
        limit=DEFAULT_LIMIT,
        delimiter=DEFAULT_DELIMITER,
        buffer_size=DEFAULT_BUFFER_SIZE
):
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

class LimitedReader(RawIOBase):

    def __init__(
            self,
            source,
            limit=DEFAULT_LIMIT,
            delimiter=DEFAULT_DELIMITER,
            buffer_size=DEFAULT_BUFFER_SIZE,
            remainder=b''
    ):
        super().__init__()
        self.source = source
        self.limit = limit
        self.delimiter = delimiter
        self.buffer = bytearray(buffer_size)
        self.remainder = remainder
        self.eof = False

    def readinto(self, output):
        if self.eof:
            return 0

        raw_data = self._read(len(output))
        read_size = len(raw_data)
        output_size = len(output)

        if read_size > self.limit:
            try:
                delimiter_index = raw_data.index(self.delimiter, max(self.limit - 1, 0))
                self.eof = True
                return self._write(output, delimiter_index + 1, raw_data)
            except ValueError:
                pass

        if read_size < output_size:
            self.eof = True
            return self._write(output, read_size, raw_data)

        return self._write(output, read_size, raw_data)

    def _read(self, size):
        if len(self.remainder) >= size:
            raw_data = self.remainder
        else:
            read_size = self.source.readinto(self.buffer)
            raw_data = self.remainder + self.buffer[:read_size]

        data, self.remainder = raw_data[:size], raw_data[size:]
        return data

    def _write(self, output, output_size, data):
        output[:], self.remainder = data[:output_size], data[output_size:]
        read_size = min(len(data), output_size)
        self.limit = self.limit - read_size
        return read_size
