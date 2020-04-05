import enum
import struct

from . import metadata


class PeerMessage(enum.Enum):
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
    """Return a lowercase representation of the message's type."""
    if not message:
        return PeerMessage.KEEPALIVE
    return PeerMessage(message[0])


def handshake(info_hash, peer_id):
    """Return the length-prefixed "handshake" message built from the input."""
    return struct.pack(
        ">B19sQ20s20s", 19, b"BitTorrent protocol", 1 << 20, info_hash, peer_id
    )


def interested():
    """Return the length-prefixed "interested" message."""
    return struct.pack(">LB", 1, PeerMessage.INTERESTED.value)


def request(block):
    """Return the length-prefixed "request" message for the given block."""
    return struct.pack(
        ">LBLLL",
        1 + 4 + 4 + 4,
        PeerMessage.REQUEST.value,
        block.piece.index,
        block.piece_offset,
        block.length,
    )


def cancel(block):
    """Return the length-prefixed "cancel" message for the given block."""
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
    """Return the piece that the given "have" message is about."""
    (piece_index,) = struct.unpack(">L", payload)
    return pieces[piece_index]


def parse_bitfield(payload, pieces):
    """Return a list of the pieces whose bits are set in the given bitfield."""
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
    """Return the pair (block, block_data) for the given "block" message."""
    piece_index, piece_offset = struct.unpack(">LL", payload[:8])
    block_data = payload[8:]
    block = metadata.Block(pieces[piece_index], piece_offset, len(block_data))
    return block, block_data
