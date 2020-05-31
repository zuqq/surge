from . import bencoding


class Message:
    def to_bytes(self):
        raise NotImplementedError

    @classmethod
    def from_bytes(cls, _):
        raise NotImplementedError


class Request(Message):
    value = 0

    def __init__(self, index):
        self.index = index

    def to_bytes(self):
        return bencoding.encode({b"msg_type": self.value, b"piece": self.index})


class Data(Message):
    value = 1

    def __init__(self, index, data):
        self.index = index
        self.data = data


class Reject(Message):
    value = 2

    def __init__(self, index):
        self.index = index


def parse(data):
    i, d = bencoding.decode_from(data, 0)
    if d[b"msg_type"] == Request.value:
        return Request(d[b"piece"])
    # TODO: Compare the `b"total_size"` key with what we expect.
    if d[b"msg_type"] == Data.value:
        return Data(d[b"piece"], data[i:])
    if d[b"msg_type"] == Reject.value:
        return Reject(d[b"piece"])
