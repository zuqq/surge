from __future__ import annotations

from .. import bencoding


class Message:
    def to_bytes(self) -> bytes:
        raise NotImplementedError


class Request(Message):
    value = 0

    def __init__(self, index: int):
        self.index = index

    def to_bytes(self) -> bytes:
        return bencoding.encode({b"msg_type": self.value, b"piece": self.index})


class Data(Message):
    value = 1

    def __init__(self, index: int, total_size: int, data: bytes):
        self.index = index
        self.total_size = total_size
        self.data = data


class Reject(Message):
    value = 2

    def __init__(self, index: int):
        self.index = index


def parse(data: bytes) -> Message:
    i, d = bencoding.decode_from(data, 0)

    if b"msg_type" not in d:
        raise ValueError("Missing key b'msg_type'.")
    value = d[b"msg_type"]
    if b"piece" not in d:
        raise ValueError("Missing key b'piece'.")
    index = d[b"piece"]

    if value == Request.value:
        return Request(index)
    if value == Data.value:
        if b"total_size" not in d:
            raise ValueError("Missing key b'total_size'.")
        return Data(index, d[b"total_size"], data[i:])
    if value == Reject.value:
        return Reject(index)
    raise ValueError("Invalid value for b'msg_type'.")
