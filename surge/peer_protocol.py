import enum
import struct

from . import metadata


class PeerMessage(enum.Enum):
    """Map peer message types to their identifiers."""

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
        return "keepalive"
    return PeerMessage(message[0]).name.lower()


def handshake(info_hash, peer_id):
    return struct.pack(
        ">B19s8s20s20s", 19, b"BitTorrent protocol", bytes(8), info_hash, peer_id
    )


def interested():
    """Return a length-prefixed "interested" message."""
    return struct.pack(">LB", 1, PeerMessage.INTERESTED.value)


def request(block):
    """Return a length-prefixed "request" message."""
    return struct.pack(
        ">LBLLL",
        1 + 4 + 4 + 4,
        PeerMessage.REQUEST.value,
        block.piece.index,
        block.piece_offset,
        block.length,
    )


def cancel(block):
    """Return a length-prefixed "cancel" message."""
    return struct.pack(
        ">LBLLL",
        1 + 4 + 4 + 4,
        PeerMessage.CANCEL.value,
        block.piece.index,
        block.piece_offset,
        block.length,
    )


def valid_handshake(message, info_hash, *, extension_protocol=False):
    # TODO
    return True


def parse_have(payload, pieces):
    (piece_index,) = struct.unpack(">L", payload)
    return pieces[piece_index]


def parse_bitfield(payload, pieces):
    available_pieces = []
    i = 0
    for b in payload:
        mask = 1 << 7
        while mask and i < len(pieces):
            if b & mask:
                available_pieces.append(pieces[i])
            mask >>= 1
            i += 1
    return available_pieces


def parse_block(payload, pieces):
    piece_index, piece_offset = struct.unpack(">LL", payload[:8])
    block_data = payload[8:]
    block = metadata.Block(pieces[piece_index], piece_offset, len(block_data))
    return block, block_data
