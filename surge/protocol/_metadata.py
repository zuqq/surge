from .. import bencoding


class Request:
    value = 0

    def __init__(self, index):
        self.index = index

    def to_bytes(self):
        return bencoding.encode({b"msg_type": self.value, b"piece": self.index})


class Data:
    value = 1

    def __init__(self, index, data):
        self.index = index
        self.data = data


class Reject:
    value = 2

    def __init__(self, index):
        self.index = index


def parse(data):
    i, d = bencoding.decode_from(data, 0)
    value = d[b"msg_type"]
    index = d[b"piece"]
    if value == Request.value:
        return Request(index)
    # TODO: Compare the `b"total_size"` key with what we expect.
    if value == Data.value:
        return Data(index, data[i:])
    if value == Reject.value:
        return Reject(index)
    raise ValueError(data)
