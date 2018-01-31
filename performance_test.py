#!/usr/bin/env python3

# this file is to be called by performance-test.sh

import random
import io
from chunk import chunk


def test():
    random.seed()
    bytes_to_read = int(2 ** (random.random() * 32767 / 1500))
    desired_size = int(2 ** (random.random() * 32767 / 1500))

    print("Testing {} bytes with a chunk size of {}".format(bytes_to_read, desired_size))

    with open('/dev/urandom', 'rb') as handle:
        content = handle.read(bytes_to_read)
    input_handle = io.BytesIO(content)
    output = io.BytesIO()

    block = b''
    for chunk_handle in chunk(input_handle, limit=desired_size, delimiter=b'\n'):
        # check previous block ends with nl (it's mandatory for every one except the last)
        if block:
            assert block[-1:] == b'\n'
            assert len(block) >= desired_size

        returned_less = False
        read_blocks = []
        while True:
            read_size = int(2**(random.random()*22))
            current_block = chunk_handle.read(read_size)
            read_blocks.append(current_block)
            assert len(current_block) <= read_size

            if current_block:
                # if something was returned, the previous one should have been exact
                assert not returned_less
            else:
                break
            returned_less = len(current_block) < read_size

        block = b''.join(read_blocks)
        assert block.rfind(b'\n', 0, -1) <= desired_size
        output.write(block)

    out = output.getvalue()
    assert content == out, "Compared {} processed bytes to {} original bytes".format(
        len(out), len(content)
    )


if __name__ == '__main__':
    test()
