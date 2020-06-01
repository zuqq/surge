from __future__ import annotations

from .. import bencoding


class Message:
    def to_bytes(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def from_bytes(cls, _: bytes) -> Message:
        # TODO: Check if the message is well-formed.
        return cls()


class Request(Message):
    value = 0

    def __init__(self, index: int):
        self.index = index

    def to_bytes(self) -> bytes:
        return bencoding.encode({b"msg_type": self.value, b"piece": self.index})


class Data(Message):
    value = 1

    def __init__(self, index: int, data: bytes):
        self.index = index
        self.data = data


class Reject(Message):
    value = 2

    def __init__(self, index: int):
        self.index = index


def parse(data: bytes) -> Message:
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
