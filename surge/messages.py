"""BitTorrent message types and parser.

Every BitTorrent message falls into one of three categories:
- messages belonging to the base protocol [BEP 0003];
- messages belonging to the extension protocol [BEP 0010];
- messages belonging to the metadata exchange protocol [BEP 0009].

These categories correspond to the classes `Message`, `ExtensionProtocol`,
and `MetadataProtocol`.

Among the messages belonging to the base protocol, `Handshake` is special: it
possesses neither length prefix nor identifier byte and must therefore be
treated separately when parsing.

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
[BEP 0010]: http://bittorrent.org/beps/bep_0010.html
[BEP 0009]: http://bittorrent.org/beps/bep_0009.html
"""

from __future__ import annotations
from typing import Dict, Optional, Sequence, Set, Tuple, Type, Union

import struct

from . import bencoding
from . import metadata


# Base class -------------------------------------------------------------------


class Message:
    """Base class for BitTorrent messages.

    Messages are (de-)serialized using `struct`. The format string is stored in
    the attribute `format`, while `fields` stores the names of the attributes
    holding the data.
    """

    format = ""
    fields: Tuple[str, ...] = ()

    def to_bytes(self) -> bytes:
        return struct.pack(self.format, *(getattr(self, f) for f in self.fields))

    @classmethod
    def from_bytes(cls, data: bytes) -> Message:
        # Default implementation for messages that are ignored.
        return cls()


# Messages without identifier byte ---------------------------------------------


class Handshake(Message):
    format = ">B19sQ20s20s"
    fields = ("pstrlen", "pstr", "reserved", "info_hash", "peer_id")

    pstrlen = 19
    pstr = b"BitTorrent protocol"
    reserved = 0

    def __init__(
            self,
            info_hash: bytes,
            peer_id: bytes,
            *,
            extension_protocol: bool = False):
        self.info_hash = info_hash
        self.peer_id = peer_id
        if extension_protocol:
            self.reserved |= 1 << 20  # See BEP 10.

    @classmethod
    def from_bytes(cls, data: bytes) -> Handshake:
        _, _, _, info_hash, peer_id = struct.unpack(cls.format, data)
        return cls(info_hash, peer_id)


class Keepalive(Message):
    format = ">L"
    fields = ("prefix",)
    prefix = 0


# Messages with identifier byte ------------------------------------------------


registry: Dict[int, Type[Message]] = {}


def register(cls: Type[Message]) -> Type[Message]:
    registry[cls.value] = cls  # type: ignore
    return cls


@register
class Choke(Message):
    format = ">LB"
    fields = ("prefix", "value")
    prefix = 1
    value = 0


@register
class Unchoke(Message):
    format = ">LB"
    fields = ("prefix", "value")
    prefix = 1
    value = 1


@register
class Interested(Message):
    format = ">LB"
    fields = ("prefix", "value")
    prefix = 1
    value = 2


@register
class NotInterested(Message):
    format = ">LB"
    fields = ("prefix", "value")
    prefix = 1
    value = 3


@register
class Have(Message):
    format = ">LBL"
    fields = ("prefix", "value", "index")
    prefix = 5
    value = 4

    def __init__(self, index: int):
        self.index = index

    @classmethod
    def from_bytes(cls, data: bytes) -> Have:
        _, _, index = struct.unpack(cls.format, data)
        return cls(index)

    def piece(self, pieces: Sequence[metadata.Piece]) -> metadata.Piece:
        return pieces[self.index]


@register
class Bitfield(Message):
    fields = ("prefix", "value", "payload")
    value = 5

    def __init__(self, payload: bytes):
        self.format = f">LB{len(payload)}s"
        self.prefix = 1 + len(payload)
        self.payload = payload

    @classmethod
    def from_bytes(cls, data: bytes) -> Bitfield:
        _, _, payload = struct.unpack(f">LB{len(data) - 5}s", data)
        return cls(payload)

    @classmethod
    def from_indices(cls, indices: Set[int], total: int) -> Bitfield:
        result = bytearray(total)
        for i in indices:
            result[i // 8] |= 1 << (7 - i % 8)
        return cls(bytes(result))

    def available(self, pieces: Sequence[metadata.Piece]) -> Set[metadata.Piece]:
        # TODO: Deprecate this method and use indices instead.
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
    fields = ("prefix", "value", "index", "begin", "length")
    prefix = 13
    value = 6

    def __init__(self, index: int, begin: int, length: int):
        self.index = index
        self.begin = begin
        self.length = length

    @classmethod
    def from_block(cls, block: metadata.Block):
        return cls(block.piece.index, block.begin, block.length)

    @classmethod
    def from_bytes(cls, data: bytes) -> Block:
        _, _, index, begin, length = struct.unpack(cls.format, data)
        return cls(index, begin, length)

    def block(self, pieces: Sequence[metadata.Piece]) -> metadata.Block:
        return metadata.Block(pieces[self.index], self.begin, self.length)


@register
class Block(Message):
    fields = ("prefix", "value", "index", "begin", "data")
    value = 7

    def __init__(self, index: int, begin: int, data: bytes):
        self.format = f">LBLL{len(data)}s"
        self.prefix = 13 + len(data)
        self.index = index
        self.begin = begin
        self.data = data

    @classmethod
    def from_block(cls, block: metadata.Block, data: bytes):
        return cls(block.piece.index, block.begin, data)

    @classmethod
    def from_bytes(cls, data: bytes) -> Block:
        # TODO: Don't shadow `data`!
        _, _, index, begin, data = struct.unpack(f">LBLL{len(data) - 13}s", data)
        return cls(index, begin, data)

    def block(self, pieces: Sequence[metadata.Piece]) -> metadata.Block:
        return metadata.Block(pieces[self.index], self.begin, len(self.data))


@register
class Cancel(Message):
    value = 8


@register
class Port(Message):
    value = 9


# Extension protocol -----------------------------------------------------------


@register
class ExtensionProtocol(Message):
    """Base class for messages belonging to the extension protocol."""

    value = 20

    def to_bytes(self) -> bytes:
        raise NotImplementedError  # pragma: no cover

    @classmethod
    def from_bytes(cls, data: bytes) -> ExtensionProtocol:
        if data[5] == ExtensionHandshake.extension_value:
            return ExtensionHandshake.from_bytes(data)
        if data[5] == MetadataProtocol.extension_value:
            return MetadataProtocol.from_bytes(data)
        raise ValueError("Unknown extension protocol identifier.")


class ExtensionHandshake(ExtensionProtocol):
    extension_value = 0

    def __init__(self, ut_metadata: int = 3, metadata_size: Optional[int] = None):
        self.ut_metadata = ut_metadata
        self.metadata_size = metadata_size

    def to_bytes(self) -> bytes:
        d = {b"m": {b"ut_metadata": self.ut_metadata}}
        if self.metadata_size is not None:
            d[b"metadata_size"] = self.metadata_size
        payload = bencoding.encode(d)
        n = len(payload)
        return struct.pack(
            f">LBB{n}s", n + 2, self.value, self.extension_value, payload
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> ExtensionHandshake:
        d = bencoding.decode(data[6:])
        return cls(d[b"m"][b"ut_metadata"], d[b"metadata_size"])


# Metadata protocol ------------------------------------------------------------


class MetadataProtocol(ExtensionProtocol):
    """Base class for messages belonging to the metadata exchange protocol."""

    extension_value = 3

    def __init__(self, ut_metadata: int = 3):
        self.extension_value = ut_metadata

    def to_bytes(self) -> bytes:
        raise NotImplementedError  # pragma: no cover

    @classmethod
    def from_bytes(cls, data: bytes) -> MetadataProtocol:
        i, d = bencoding.decode_from(data, 6)
        if b"msg_type" not in d:
            raise ValueError("Missing key b'msg_type'.")
        metadata_value = d[b"msg_type"]
        if b"piece" not in d:
            raise ValueError("Missing key b'piece'.")
        index = d[b"piece"]
        if metadata_value == MetadataRequest.metadata_value:
            return MetadataRequest(index)
        if metadata_value == MetadataData.metadata_value:
            if b"total_size" not in d:
                raise ValueError("Missing key b'total_size'.")
            return MetadataData(index, d[b"total_size"], data[i:])
        if metadata_value == MetadataReject.metadata_value:
            return MetadataReject(index)
        raise ValueError("Invalid value for b'msg_type'.")


class MetadataRequest(MetadataProtocol):
    metadata_value = 0

    def __init__(self, index: int, ut_metadata: int = 3):
        super().__init__(ut_metadata)
        self.index = index

    def to_bytes(self) -> bytes:
        payload = bencoding.encode(
            {b"msg_type": self.metadata_value, b"piece": self.index}
        )
        n = len(payload)
        return struct.pack(
            f">LBB{n}s", n + 2, self.value, self.extension_value, payload
        )


class MetadataData(MetadataProtocol):
    metadata_value = 1

    def __init__(self, index: int, total_size: int, data: bytes, ut_metadata: int = 3):
        super().__init__(ut_metadata)
        self.index = index
        self.total_size = total_size
        self.data = data

    def to_bytes(self) -> bytes:
        d = {
            b"msg_type": self.metadata_value,
            b"piece": self.index,
            b"total_size": self.total_size,
        }
        payload = bencoding.encode(d)
        m = len(payload)
        n = len(self.data)
        return struct.pack(
            f">LBB{m}s{n}s",
            m + n + 2,
            self.value,
            self.extension_value,
            payload,
            self.data,
        )


class MetadataReject(MetadataProtocol):
    metadata_value = 2

    def __init__(self, index: int, ut_metadata: int = 3):
        super().__init__(ut_metadata)
        self.index = index

    def to_bytes(self) -> bytes:
        payload = bencoding.encode(
            {b"msg_type": self.metadata_value, b"piece": self.index}
        )
        n = len(payload)
        return struct.pack(
            f">LBB{n}s", n + 2, self.value, self.extension_value, payload
        )


# Parser -----------------------------------------------------------------------


def parse_handshake(data: bytes) -> Handshake:
    """Parse a BitTorrent handshake."""
    return Handshake.from_bytes(data)


def parse(data: bytes) -> Message:
    """Parse a BitTorrent message.

    Raise `ValueError` if `data` is not a valid BitTorrent message.
    """
    n = int.from_bytes(data[:4], "big")
    if len(data) != 4 + n:
        raise ValueError("Incorrect length prefix.")
    try:
        cls = registry[data[4]]
    except IndexError:
        return Keepalive()
    except KeyError:
        raise ValueError("Unknown message identifier.")
    return cls.from_bytes(data)
