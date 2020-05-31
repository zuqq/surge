import enum
import struct

from . import bencoding
from . import metadata_protocol


class Message:
    pass


_registry = {}


def register(cls):
    _registry[cls.value] = cls
    return cls


@register
class Handshake:
    value = 0

    def __init__(self, ut_metadata=3, metadata_size=None):
        self.ut_metadata = ut_metadata
        self.metadata_size = metadata_size

    def to_bytes(self):
        # TODO: Add `self.metadata_size` if it's not `None`.
        payload = bencoding.encode({b"m": {b"ut_metadata": self.ut_metadata}})
        n = len(payload)
        return struct.pack(f">B{n}s", self.value, payload)

    @classmethod
    def from_bytes(cls, payload):
        d = bencoding.decode(payload[1:])
        return cls(d[b"m"][b"ut_metadata"], d[b"metadata_size"])


@register
class Metadata:
    value = 3

    def __init__(self, message, value=None):
        self.metadata_message = message
        if value is not None:
            self.value = value

    def to_bytes(self):
        payload = self.metadata_message.to_bytes()
        n = len(payload)
        return struct.pack(f">B{n}s", self.value, payload)

    @classmethod
    def from_bytes(cls, payload):
        return cls(metadata_protocol.parse(payload[1:]))


def parse(payload):
    return _registry[payload[0]].from_bytes(payload)
