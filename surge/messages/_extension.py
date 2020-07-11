from __future__ import annotations
from typing import Optional

import struct

from . import _metadata
from .. import bencoding


class Message:
    def to_bytes(self) -> bytes:
        raise NotImplementedError  # pragma: no cover

    @classmethod
    def from_bytes(cls, payload: bytes) -> Message:
        raise NotImplementedError  # pragma: no cover


class Handshake(Message):
    value = 0

    def __init__(self, ut_metadata: int = 3, metadata_size: Optional[int] = None):
        self.ut_metadata = ut_metadata
        self.metadata_size = metadata_size

    def to_bytes(self) -> bytes:
        # TODO: Add `self.metadata_size` if it's not `None`.
        payload = bencoding.encode({b"m": {b"ut_metadata": self.ut_metadata}})
        return struct.pack(f">B{len(payload)}s", self.value, payload)

    @classmethod
    def from_bytes(cls, payload: bytes) -> Handshake:
        d = bencoding.decode(payload[1:])
        return cls(d[b"m"][b"ut_metadata"], d[b"metadata_size"])


class Metadata(Message):
    value = 3

    def __init__(self, metadata_message: _metadata.Message, value=None):
        self.metadata_message = metadata_message
        if value is not None:
            self.value = value

    def to_bytes(self) -> bytes:
        payload = self.metadata_message.to_bytes()
        return struct.pack(f">B{len(payload)}s", self.value, payload)

    @classmethod
    def from_bytes(cls, payload: bytes) -> Metadata:
        return cls(_metadata.parse(payload[1:]))


def parse(payload: bytes) -> Message:
    if payload[0] == Handshake.value:
        return Handshake.from_bytes(payload)
    if payload[0] == Metadata.value:
        return Metadata.from_bytes(payload)
    raise ValueError("Unknown extension protocol identifier.")
