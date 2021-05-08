"""BitTorrent message types and parser.

Every supported message has its own class, with methods `from_bytes` and
`to_bytes` for (de-)serialization.

Handshakes are special because they are only type of message without length
prefix and identifier byte; use `parse_handshake` to parse them. All other
messages are handled by the function `parse`.
"""

from typing import ClassVar, Optional

import dataclasses
import struct

from . import bencoding


@dataclasses.dataclass
class Handshake:
    pstrlen: ClassVar[int] = 19
    pstr: ClassVar[bytes] = b"BitTorrent protocol"
    reserved: int
    info_hash: bytes
    peer_id: bytes

    @classmethod
    def from_bytes(cls, raw_message):
        _, _, reserved, info_hash, peer_id = struct.unpack(">B19sQ20s20s", raw_message)
        return cls(reserved, info_hash, peer_id)

    def to_bytes(self):
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
    def from_bytes(cls, _):
        return cls()  # pragma: no cover

    def to_bytes(self):
        return struct.pack(">L", self.prefix)


@dataclasses.dataclass
class Choke:
    prefix: ClassVar[int] = 1
    value: ClassVar[int] = 0

    @classmethod
    def from_bytes(cls, _):
        return cls()

    def to_bytes(self):
        return struct.pack(">LB", self.prefix, self.value)


@dataclasses.dataclass
class Unchoke:
    prefix: ClassVar[int] = 1
    value: ClassVar[int] = 1

    @classmethod
    def from_bytes(cls, _):
        return cls()

    def to_bytes(self):
        return struct.pack(">LB", self.prefix, self.value)


@dataclasses.dataclass
class Interested:
    prefix: ClassVar[int] = 1
    value: ClassVar[int] = 2

    @classmethod
    def from_bytes(cls, _):
        return cls()

    def to_bytes(self):
        return struct.pack(">LB", self.prefix, self.value)


@dataclasses.dataclass
class NotInterested:
    prefix: ClassVar[int] = 1
    value: ClassVar[int] = 3

    @classmethod
    def from_bytes(cls, _):
        return cls()

    def to_bytes(self):
        return struct.pack(">LB", self.prefix, self.value)


@dataclasses.dataclass
class Have:
    prefix: ClassVar[int] = 5
    value: ClassVar[int] = 4
    index: int

    @classmethod
    def from_bytes(cls, raw_message):
        _, _, index = struct.unpack(">LBL", raw_message)
        return cls(index)

    def to_bytes(self):
        return struct.pack(">LBL", self.prefix, self.value, self.index)


@dataclasses.dataclass
class Bitfield:
    value: ClassVar[int] = 5
    bitfield: bytes

    @classmethod
    def from_bytes(cls, raw_message):
        _, _, bitfield = struct.unpack(f">LB{len(raw_message) - 5}s", raw_message)
        return cls(bitfield)

    @classmethod
    def from_indices(cls, indices, length):
        result = bytearray((length + 7) // 8)
        for i in indices:
            result[i // 8] |= 1 << (7 - i % 8)
        return cls(bytes(result))

    def to_bytes(self):
        n = len(self.bitfield)
        return struct.pack(f">LB{n}s", n + 1, self.value, self.bitfield)

    def to_indices(self):
        result = set()
        i = 0
        for b in self.bitfield:
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
    def from_block(cls, block):
        return cls(block.piece.index, block.begin, block.length)

    @classmethod
    def from_bytes(cls, raw_message):
        _, _, index, begin, length = struct.unpack(">LBLLL", raw_message)
        return cls(index, begin, length)

    def to_bytes(self):
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
    def from_block(cls, block, raw_message):
        return cls(block.piece.index, block.begin, raw_message)

    @classmethod
    def from_bytes(cls, raw_message):
        _, _, index, begin, data = struct.unpack(
            f">LBLL{len(raw_message) - 13}s", raw_message
        )
        return cls(index, begin, data)

    def to_bytes(self):
        n = len(self.data)
        return struct.pack(
            f">LBLL{n}s", n + 9, self.value, self.index, self.begin, self.data
        )


@dataclasses.dataclass
class Cancel:
    prefix: ClassVar[int] = 13
    value: ClassVar[int] = 8
    index: int
    begin: int
    length: int

    @classmethod
    def from_block(cls, block):
        return cls(block.piece.index, block.begin, block.length)

    @classmethod
    def from_bytes(cls, raw_message):
        _, _, index, begin, length = struct.unpack(">LBLLL", raw_message)
        return cls(index, begin, length)

    def to_bytes(self):
        return struct.pack(
            ">LBLLL", self.prefix, self.value, self.index, self.begin, self.length
        )


@dataclasses.dataclass
class ExtensionProtocol:
    """Base class for [extension protocol][BEP 0010] messages.

    [BEP 0010]: http://bittorrent.org/beps/bep_0010.html
    """

    value: ClassVar[int] = 20

    @classmethod
    def from_bytes(cls, raw_message):
        if raw_message[5] == ExtensionHandshake.extension_value:
            return ExtensionHandshake.from_bytes(raw_message)
        if raw_message[5] == MetadataProtocol.extension_value:
            return MetadataProtocol.from_bytes(raw_message)
        raise ValueError("Unknown extension protocol identifier.")


@dataclasses.dataclass
class ExtensionHandshake(ExtensionProtocol):
    extension_value: ClassVar[int] = 0
    ut_metadata: int = 3
    metadata_size: Optional[int] = None

    def to_bytes(self):
        d = {b"m": {b"ut_metadata": self.ut_metadata}}
        if self.metadata_size is not None:
            d[b"metadata_size"] = self.metadata_size
        payload = bencoding.encode(d)
        n = len(payload)
        return struct.pack(
            f">LBB{n}s", n + 2, self.value, self.extension_value, payload
        )

    @classmethod
    def from_bytes(cls, raw_message):
        d = bencoding.decode(raw_message[6:])
        return cls(d[b"m"][b"ut_metadata"], d.get(b"metadata_size", None))


@dataclasses.dataclass
class MetadataProtocol(ExtensionProtocol):
    """Base class for [metadata exchange protocol][BEP 0009] messages.

    [BEP 0009]: http://bittorrent.org/beps/bep_0009.html
    """

    extension_value: ClassVar[int] = 3

    @classmethod
    def from_bytes(cls, raw_message):
        i, d = bencoding.decode_from(raw_message, 6)
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
            return MetadataData(index, d[b"total_size"], raw_message[i:])
        if metadata_value == MetadataReject.metadata_value:
            return MetadataReject(index)
        raise ValueError("Invalid value for b'msg_type'.")


@dataclasses.dataclass
class MetadataRequest(MetadataProtocol):
    metadata_value: ClassVar[int] = 0
    index: int
    ut_metadata: dataclasses.InitVar[Optional[int]] = None

    def __post_init__(self, ut_metadata):
        if ut_metadata is not None:
            self.extension_value = ut_metadata

    def to_bytes(self):
        payload = bencoding.encode(
            {b"msg_type": self.metadata_value, b"piece": self.index}
        )
        n = len(payload)
        return struct.pack(
            f">LBB{n}s", n + 2, self.value, self.extension_value, payload
        )


@dataclasses.dataclass
class MetadataData(MetadataProtocol):
    metadata_value: ClassVar[int] = 1
    index: int
    total_size: int
    data: bytes
    ut_metadata: dataclasses.InitVar[Optional[int]] = None

    def __post_init__(self, ut_metadata):
        if ut_metadata is not None:
            self.extension_value = ut_metadata

    def to_bytes(self):
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
    metadata_value: ClassVar[int] = 2
    index: int
    ut_metadata: dataclasses.InitVar[Optional[int]] = None

    def __post_init__(self, ut_metadata):
        if ut_metadata is not None:
            self.extension_value = ut_metadata

    def to_bytes(self):
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
    20: ExtensionProtocol,
}


def parse_handshake(raw_message):
    """Parse a BitTorrent handshake."""
    return Handshake.from_bytes(raw_message)


def parse(raw_message):
    """Parse a BitTorrent message.

    Raise `ValueError` if `raw_message` is not a valid BitTorrent message.
    """
    n = int.from_bytes(raw_message[:4], "big")
    if len(raw_message) != 4 + n:
        raise ValueError("Incorrect length prefix.")
    try:
        cls = MESSAGE_TYPE[raw_message[4]]
    except IndexError:
        return Keepalive()
    except KeyError as exc:
        raise ValueError("Unknown message identifier.") from exc
    return cls.from_bytes(raw_message)
