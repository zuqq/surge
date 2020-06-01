import struct

from . import _extension
from .. import metadata


_registry = {}


def register(cls):
    _registry[cls.value] = cls
    return cls


class Message:
    format = ""
    fields = []

    def to_bytes(self):
        return struct.pack(self.format, *(getattr(self, f) for f in self.fields))

    @classmethod
    def from_bytes(cls, _):
        # TODO: Check if the message is well-formed.
        return cls()


class Handshake(Message):
    format = ">B19sQ20s20s"
    fields = ["pstrlen", "pstr", "reserved", "info_hash", "peer_id"]

    pstrlen = 19
    pstr = b"BitTorrent protocol"
    reserved = 1 << 20

    def __init__(self, info_hash, peer_id):
        self.info_hash = info_hash
        self.peer_id = peer_id

    @classmethod
    def from_bytes(cls, data):
        _, _, _, info_hash, peer_id = struct.unpack(cls.format, data)
        return cls(info_hash, peer_id)


class Keepalive(Message):
    format = ">L"
    fields = ["length"]
    length = 0


@register
class Choke(Message):
    format = ">LB"
    fields = ["length", "value"]
    length = 1
    value = 0


@register
class Unchoke(Message):
    format = ">LB"
    fields = ["length", "value"]
    length = 1
    value = 1


@register
class Interested(Message):
    format = ">LB"
    fields = ["length", "value"]
    length = 1
    value = 2


@register
class NotInterested(Message):
    format = ">LB"
    fields = ["length", "value"]
    length = 1
    value = 3


@register
class Have(Message):
    format = ">LBL"
    fields = ["length", "value", "index"]
    length = 5
    value = 4

    def __init__(self, index):
        self.index = index

    @classmethod
    def from_bytes(cls, data):
        _, _, index = struct.unpack(cls.format, data)
        return cls(index)

    def piece(self, pieces):
        return pieces[self.index]


@register
class Bitfield(Message):
    fields = ["length", "value", "payload"]
    value = 5

    def __init__(self, payload):
        self.length = 1 + len(payload)
        self.payload = payload

        self.format = f"LB{self.length}s"

    @classmethod
    def from_bytes(cls, data):
        _, _, payload = struct.unpack(f">LB{len(data) - 4 - 1}s", data)
        return cls(payload)

    def available(self, pieces):
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
    fields = ["length", "value", "index", "offset", "data_length"]
    length = 13
    value = 6

    def __init__(self, block):
        self.index = block.piece.index
        self.offset = block.piece_offset
        self.data_length = block.length


@register
class Block(Message):
    fields = ["length", "value", "index", "offset", "data"]
    value = 7

    def __init__(self, index, offset, data):
        self.index = index
        self.offset = offset
        self.data = data

        self.format = f">LBLL{len(self.data)}s"

    @classmethod
    def from_bytes(cls, data):
        _, _, index, offset, data = struct.unpack(
            f">LBLL{len(data) - 4 - 1 - 4 - 4}s", data
        )
        return cls(index, offset, data)

    def block(self, pieces):
        return metadata.Block(pieces[self.index], self.offset, len(self.data))


@register
class Cancel(Message):
    value = 8


@register
class Port(Message):
    value = 9


@register
class ExtensionProtocol(Message):
    value = 20

    def __init__(self, extension_message):
        self.extension_message = extension_message

    def to_bytes(self):
        payload = self.extension_message.to_bytes()
        n = len(payload)
        return struct.pack(f">LB{n}s", n + 1, self.value, payload)

    @classmethod
    def from_bytes(cls, data):
        return cls(_extension.parse(data[5:]))


def parse(data):
    try:
        cls = _registry[data[4]]
    except IndexError:
        return Keepalive()
    message = cls.from_bytes(data)
    if cls is not ExtensionProtocol:
        return message
    extension_message = message.extension_message
    if isinstance(extension_message, _extension.Handshake):
        return extension_message
    if isinstance(extension_message, _extension.Metadata):
        return extension_message.metadata_message
    raise ValueError(data)
