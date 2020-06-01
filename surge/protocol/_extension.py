import struct

from . import _metadata
from .. import bencoding


class Handshake:
    value = 0

    def __init__(self, ut_metadata=3, metadata_size=None):
        self.ut_metadata = ut_metadata
        self.metadata_size = metadata_size

    def to_bytes(self):
        # TODO: Add `self.metadata_size` if it's not `None`.
        payload = bencoding.encode({b"m": {b"ut_metadata": self.ut_metadata}})
        return struct.pack(f">B{len(payload)}s", self.value, payload)

    @classmethod
    def from_bytes(cls, payload):
        d = bencoding.decode(payload[1:])
        return cls(d[b"m"][b"ut_metadata"], d[b"metadata_size"])


class Metadata:
    value = 3

    def __init__(self, metadata_message, value=None):
        self.metadata_message = metadata_message
        if value is not None:
            self.value = value

    def to_bytes(self):
        payload = self.metadata_message.to_bytes()
        return struct.pack(f">B{len(payload)}s", self.value, payload)

    @classmethod
    def from_bytes(cls, payload):
        return cls(_metadata.parse(payload[1:]))


def parse(payload):
    if payload[0] == Handshake.value:
        return Handshake.from_bytes(payload)
    if payload[0] == Metadata.value:
        return Metadata.from_bytes(payload)
    raise ValueError(payload)
