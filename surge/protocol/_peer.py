from __future__ import annotations
from typing import Dict, List, Set, Type, Union

import struct

from . import _extension
from . import _metadata
from .. import metadata


class Message:
    format = ""
    fields: List[str] = []

    def to_bytes(self) -> bytes:
        return struct.pack(self.format, *(getattr(self, f) for f in self.fields))

    @classmethod
    def from_bytes(cls, data: bytes) -> Message:
        # Default implementation for messages that are ignored.
        return cls()


_registry: Dict[int, Type[Message]] = {}


def register(cls: Type[Message]) -> Type[Message]:
    _registry[cls.value] = cls  # type: ignore
    return cls


class Handshake(Message):
    format = ">B19sQ20s20s"
    fields = ["pstrlen", "pstr", "reserved", "info_hash", "peer_id"]

    pstrlen = 19
    pstr = b"BitTorrent protocol"
    reserved = 1 << 20

    def __init__(self, info_hash: bytes, peer_id: bytes):
        self.info_hash = info_hash
        self.peer_id = peer_id

    @classmethod
    def from_bytes(cls, data: bytes) -> Handshake:
        _, _, _, info_hash, peer_id = struct.unpack(cls.format, data)
        return cls(info_hash, peer_id)


class Keepalive(Message):
    format = ">L"
    fields = ["prefix"]
    prefix = 0


@register
class Choke(Message):
    format = ">LB"
    fields = ["prefix", "value"]
    prefix = 1
    value = 0


@register
class Unchoke(Message):
    format = ">LB"
    fields = ["prefix", "value"]
    prefix = 1
    value = 1


@register
class Interested(Message):
    format = ">LB"
    fields = ["prefix", "value"]
    prefix = 1
    value = 2


@register
class NotInterested(Message):
    format = ">LB"
    fields = ["prefix", "value"]
    prefix = 1
    value = 3


@register
class Have(Message):
    format = ">LBL"
    fields = ["prefix", "value", "index"]
    prefix = 5
    value = 4

    def __init__(self, index: int):
        self.index = index

    @classmethod
    def from_bytes(cls, data: bytes) -> Have:
        _, _, index = struct.unpack(cls.format, data)
        return cls(index)

    def piece(self, pieces: List[metadata.Piece]) -> metadata.Piece:
        return pieces[self.index]


@register
class Bitfield(Message):
    fields = ["prefix", "value", "payload"]
    value = 5

    def __init__(self, payload: bytes):
        self.prefix = 1 + len(payload)
        self.payload = payload

        self.format = f"LB{len(payload)}s"

    @classmethod
    def from_bytes(cls, data: bytes) -> Bitfield:
        _, _, payload = struct.unpack(f">LB{len(data) - 4 - 1}s", data)
        return cls(payload)

    def available(self, pieces: List[metadata.Piece]) -> Set[metadata.Piece]:
        result = set()
        i = 0
        for b in self.payload:
            mask = 1 << 7
            while mask and i < len(pieces):
                if b & mask:
                    result.add(pieces[i])
                mask >>= 1
                i += 1
        return result


@register
class Request(Message):
    format = ">LBLLL"
    fields = ["prefix", "value", "index", "begin", "length"]
    prefix = 13
    value = 6

    def __init__(self, block: metadata.Block):
        self.index = block.piece.index
        self.begin = block.begin
        self.length = block.length


@register
class Block(Message):
    fields = ["prefix", "value", "index", "begin", "data"]
    value = 7

    def __init__(self, index: int, begin: int, data: bytes):
        self.index = index
        self.begin = begin
        self.data = data

        self.format = f">LBLL{len(self.data)}s"

    @classmethod
    def from_bytes(cls, data: bytes) -> Block:
        _, _, index, begin, data = struct.unpack(
            f">LBLL{len(data) - 4 - 1 - 4 - 4}s", data
        )
        return cls(index, begin, data)

    def block(self, pieces: List[metadata.Piece]) -> metadata.Block:
        return metadata.Block(pieces[self.index], self.begin, len(self.data))


@register
class Cancel(Message):
    value = 8


@register
class Port(Message):
    value = 9


@register
class ExtensionProtocol(Message):
    value = 20

    def __init__(self, extension_message: _extension.Message):
        self.extension_message = extension_message

    def to_bytes(self) -> bytes:
        payload = self.extension_message.to_bytes()
        n = len(payload)
        return struct.pack(f">LB{n}s", n + 1, self.value, payload)

    @classmethod
    def from_bytes(cls, data: bytes) -> ExtensionProtocol:
        return cls(_extension.parse(data[5:]))


def parse(data: bytes) -> Union[Message, _extension.Message, _metadata.Message]:
    n = int.from_bytes(data[:4], "big")
    if len(data) != 4 + n:
        raise ValueError("Incorrect length prefix.")
    try:
        cls = _registry[data[4]]
    except IndexError:
        return Keepalive()
    except KeyError:
        raise ValueError("Unknown message identifier.")
    message = cls.from_bytes(data)

    if cls is not ExtensionProtocol:
        return message

    # The extension protocol contains nested messages. We unpack them because
    # that makes implementing the protocol state machine more convenient.
    extension_message = message.extension_message  # type: ignore
    if isinstance(extension_message, _extension.Handshake):
        return extension_message
    if isinstance(extension_message, _extension.Metadata):
        return extension_message.metadata_message
    raise ValueError("Unkown extension message.")
