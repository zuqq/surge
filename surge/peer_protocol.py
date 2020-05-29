import enum
import struct

from . import metadata


class Message(enum.Enum):
    """Map peer message types to their identifiers."""

    KEEPALIVE = None
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    BLOCK = 7
    CANCEL = 8
    PORT = 9
    EXTENSION_PROTOCOL = 20


def message_type(message):
    if not message:
        return Message.KEEPALIVE
    return Message(message[0])


def handshake(info_hash, peer_id):
    """Return the length-prefixed "handshake" message built from the input."""
    return struct.pack(
        ">B19sQ20s20s", 19, b"BitTorrent protocol", 1 << 20, info_hash, peer_id
    )


def interested():
    """Return the length-prefixed "interested" message."""
    return struct.pack(">LB", 1, Message.INTERESTED.value)


def request(block):
    """Return the length-prefixed "request" message for the given block."""
    return struct.pack(
        ">LBLLL",
        1 + 4 + 4 + 4,
        Message.REQUEST.value,
        block.piece.index,
        block.piece_offset,
        block.length,
    )


def cancel(block):
    """Return the length-prefixed "cancel" message for the given block."""
    return struct.pack(
        ">LBLLL",
        1 + 4 + 4 + 4,
        Message.CANCEL.value,
        block.piece.index,
        block.piece_offset,
        block.length,
    )


def parse_handshake(payload):
    try:
        pstrlen, pstr, reserved, info_hash, peer_id = struct.unpack(
            ">B19sQ20s20s", payload
        )
    except struct.error:
        raise ValueError("Wrong length.")
    if pstrlen != 19 or pstr != b"BitTorrent protocol":
        raise ValueError("Wrong protocol string.")
    return (reserved, info_hash, peer_id)


def parse_have(payload, pieces):
    """Return the piece that the given "have" message is about."""
    (piece_index,) = struct.unpack(">L", payload)
    return pieces[piece_index]


def parse_bitfield(payload, pieces):
    """Return a list of the pieces whose bits are set in the given bitfield."""
    available = set()
    i = 0
    for b in payload:
        mask = 1 << 7
        while mask and i < len(pieces):
            if b & mask:
                available.add(pieces[i])
            mask >>= 1
            i += 1
    return available


def parse_block(payload, pieces):
    """Return the pair (block, block_data) for the given "block" message."""
    piece_index, piece_offset = struct.unpack(">LL", payload[:8])
    block_data = payload[8:]
    block = metadata.Block(pieces[piece_index], piece_offset, len(block_data))
    return block, block_data


def parse(message, pieces):
    """Return the pair (type, content) for the given message."""
    mt = message_type(message)
    if mt == Message.HAVE:
        return (mt, parse_have(message[1:], pieces))
    elif mt == Message.BITFIELD:
        return (mt, parse_bitfield(message[1:], pieces))
    elif mt == Message.BLOCK:
        return (mt, parse_block(message[1:], pieces))
    return (mt, None)
