"""BitTorrent message types and parser.

Every supported message has its own class, with methods `from_bytes` and
`to_bytes` for (de-)serialization. In order to parse a message of unknown
type that is not a `Handshake`, use the function `parse`. Handshakes are
handled by the separate function `parse_handshake` because they are the only
type of message without length prefix and identifier byte.
"""

from __future__ import annotations
from typing import ClassVar, Dict, Iterable, Optional, Set, Union

import dataclasses
import struct

from . import _metadata
from . import bencoding


@dataclasses.dataclass
class Handshake:
    pstrlen: ClassVar[int] = 19
    pstr: ClassVar[bytes] = b"BitTorrent protocol"
    reserved: int
    info_hash: bytes
    peer_id: bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> Handshake:
        _, _, reserved, info_hash, peer_id = struct.unpack(">B19sQ20s20s", data)
        return cls(reserved, info_hash, peer_id)

    def to_bytes(self) -> bytes:
        return struct.pack(
            ">B19sQ20s20s",
            self.pstrlen,
            self.pstr,
            self.reserved,
            self.info_hash,
            self.peer_id,
        )


@dataclasses.dataclass
class Keepalive:
    prefix: ClassVar[int] = 0

    @classmethod
    def from_bytes(cls, _: bytes) -> Keepalive:
        return cls()

    def to_bytes(self) -> bytes:
        return struct.pack(">L", self.prefix)


@dataclasses.dataclass
class Choke:
    prefix: ClassVar[int] = 1
    value: ClassVar[int] = 0

    @classmethod
    def from_bytes(cls, _: bytes) -> Choke:
        return cls()

    def to_bytes(self) -> bytes:
        return struct.pack(">LB", self.prefix, self.value)


@dataclasses.dataclass
class Unchoke:
    prefix: ClassVar[int] = 1
    value: ClassVar[int] = 1

    @classmethod
    def from_bytes(cls, _: bytes) -> Unchoke:
        return cls()

    def to_bytes(self) -> bytes:
        return struct.pack(">LB", self.prefix, self.value)


@dataclasses.dataclass
class Interested:
    prefix: ClassVar[int] = 1
    value: ClassVar[int] = 2

    @classmethod
    def from_bytes(cls, _: bytes) -> Interested:
        return cls()

    def to_bytes(self) -> bytes:
        return struct.pack(">LB", self.prefix, self.value)


@dataclasses.dataclass
class NotInterested:
    prefix: ClassVar[int] = 1
    value: ClassVar[int] = 3

    @classmethod
    def from_bytes(cls, _: bytes) -> NotInterested:
        return cls()

    def to_bytes(self) -> bytes:
        return struct.pack(">LB", self.prefix, self.value)


@dataclasses.dataclass
class Have:
    prefix: ClassVar[int] = 5
    value: ClassVar[int] = 4
    index: int

    @classmethod
    def from_bytes(cls, data: bytes) -> Have:
        _, _, index = struct.unpack(">LBL", data)
        return cls(index)

    def to_bytes(self) -> bytes:
        return struct.pack(">LBL", self.prefix, self.value, self.index)


@dataclasses.dataclass
class Bitfield:
    value: ClassVar[int] = 5
    payload: bytes

    @classmethod
    def from_bytes(cls, data: bytes) -> Bitfield:
        _, _, payload = struct.unpack(f">LB{len(data) - 5}s", data)
        return cls(payload)

    @classmethod
    def from_indices(cls, indices: Iterable[int], total: int) -> Bitfield:
        result = bytearray((total + 7) // 8)
        for i in indices:
            result[i // 8] |= 1 << (7 - i % 8)
        return cls(bytes(result))

    def to_bytes(self) -> bytes:
        n = len(self.payload)
        return struct.pack(f">LB{n}s", n + 1, self.value, self.payload)

    def to_indices(self) -> Set[int]:
        result = set()
        i = 0
        for b in self.payload:
            mask = 1 << 7
            while mask:
                if b & mask:
                    result.add(i)
                mask >>= 1
                i += 1
        return result


@dataclasses.dataclass
class Request:
    prefix: ClassVar[int] = 13
    value: ClassVar[int] = 6
    index: int
    begin: int
    length: int

    @classmethod
    def from_block(cls, block: _metadata.Block):
        return cls(block.piece.index, block.begin, block.length)

    @classmethod
    def from_bytes(cls, data: bytes) -> Request:
        _, _, index, begin, length = struct.unpack(">LBLLL", data)
        return cls(index, begin, length)

    def to_bytes(self) -> bytes:
        return struct.pack(
            ">LBLLL", self.prefix, self.value, self.index, self.begin, self.length
        )


@dataclasses.dataclass
class Block:
    value: ClassVar[int] = 7
    index: int
    begin: int
    data: bytes

    @classmethod
    def from_block(cls, block: _metadata.Block, data: bytes):
        return cls(block.piece.index, block.begin, data)

    @classmethod
    def from_bytes(cls, data: bytes) -> Block:
        # TODO: Don't shadow `data`!
        _, _, index, begin, data = struct.unpack(f">LBLL{len(data) - 13}s", data)
        return cls(index, begin, data)

    def to_bytes(self) -> bytes:
        n = len(self.data)
        return struct.pack(
            f">LBLL{n}s", n + 9, self.value, self.index, self.begin, self.data
        )


@dataclasses.dataclass
class Cancel:
    value: ClassVar[int] = 8

    @classmethod
    def from_bytes(cls, _: bytes) -> Cancel:
        return cls()


@dataclasses.dataclass
class Port:
    value: ClassVar[int] = 9

    @classmethod
    def from_bytes(cls, _: bytes) -> Port:
        return cls()


@dataclasses.dataclass
class ExtensionProtocol:
    """Base class for [extension protocol][BEP 0010] messages.

    [BEP 0010]: http://bittorrent.org/beps/bep_0010.html
    """

    value: ClassVar[int] = 20

    @classmethod
    def from_bytes(cls, data: bytes) -> ExtensionMessage:
        if data[5] == ExtensionHandshake.extension_value:
            return ExtensionHandshake.from_bytes(data)
        if data[5] == MetadataProtocol.extension_value:
            return MetadataProtocol.from_bytes(data)
        raise ValueError("Unknown extension protocol identifier.")


@dataclasses.dataclass
class ExtensionHandshake(ExtensionProtocol):
    extension_value: ClassVar[int] = 0
    ut_metadata: int = 3
    metadata_size: Optional[int] = None

    def to_bytes(self) -> bytes:
        d: Dict[bytes, Union[Dict[bytes, int], int]]
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


@dataclasses.dataclass
class MetadataProtocol:
    """Base class for [metadata exchange protocol][BEP 0009] messages.

    [BEP 0009]: http://bittorrent.org/beps/bep_0009.html
    """

    extension_value: ClassVar[int] = 3

    @classmethod
    def from_bytes(cls, data: bytes) -> MetadataMessage:
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


@dataclasses.dataclass
class MetadataRequest:
    value: ClassVar[int] = 20
    extension_value: ClassVar[int] = 3
    metadata_value: ClassVar[int] = 0
    index: int
    ut_metadata: dataclasses.InitVar[Optional[int]] = None

    def __post_init__(self, ut_metadata):
        if ut_metadata is not None:
            self.extension_value = ut_metadata

    def to_bytes(self) -> bytes:
        payload = bencoding.encode(
            {b"msg_type": self.metadata_value, b"piece": self.index}
        )
        n = len(payload)
        return struct.pack(
            f">LBB{n}s", n + 2, self.value, self.extension_value, payload
        )


@dataclasses.dataclass
class MetadataData:
    value: ClassVar[int] = 20
    extension_value: ClassVar[int] = 3
    metadata_value: ClassVar[int] = 1
    index: int
    total_size: int
    data: bytes
    ut_metadata: dataclasses.InitVar[Optional[int]] = None

    def __post_init__(self, ut_metadata):
        if ut_metadata is not None:
            self.extension_value = ut_metadata

    def to_bytes(self) -> bytes:
        payload = bencoding.encode(
            {
                b"msg_type": self.metadata_value,
                b"piece": self.index,
                b"total_size": self.total_size,
            }
        )
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


@dataclasses.dataclass
class MetadataReject(MetadataProtocol):
    value: ClassVar[int] = 20
    extension_value: ClassVar[int] = 3
    metadata_value: ClassVar[int] = 2
    index: int
    ut_metadata: dataclasses.InitVar[Optional[int]] = None

    def __post_init__(self, ut_metadata):
        if ut_metadata is not None:
            self.extension_value = ut_metadata

    def to_bytes(self) -> bytes:
        payload = bencoding.encode(
            {b"msg_type": self.metadata_value, b"piece": self.index}
        )
        n = len(payload)
        return struct.pack(
            f">LBB{n}s", n + 2, self.value, self.extension_value, payload
        )


MESSAGE_TYPE = {
    0: Choke,
    1: Unchoke,
    2: Interested,
    3: NotInterested,
    4: Have,
    5: Bitfield,
    6: Request,
    7: Block,
    8: Cancel,
    9: Port,
    20: ExtensionProtocol,
}
Message = Union[
    Keepalive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have,
    Bitfield,
    Request,
    Block,
    Cancel,
    Port,
    ExtensionProtocol,
]
MetadataMessage = Union[MetadataRequest, MetadataData, MetadataReject]
ExtensionMessage = Union[ExtensionHandshake, MetadataMessage]


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
        cls = MESSAGE_TYPE[data[4]]
    except IndexError:
        return Keepalive()
    except KeyError as exc:
        raise ValueError("Unknown message identifier.") from exc
    return cls.from_bytes(data)
