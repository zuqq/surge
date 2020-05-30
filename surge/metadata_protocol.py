from typing import Tuple
import enum
import struct

from . import bencoding


class Message(enum.Enum):
    """Map metadata protocol message types to their identifiers."""

    REQUEST = 0
    DATA = 1
    REJECT = 2


def parse_message(message: bytes) -> Tuple[Message, bytes]:
    i, d = bencoding.decode_from(message, 0)
    return (d, message[i:])


def request(index: int, ut_metadata: int) -> bytes:
    payload = bencoding.encode({b"msg_type": Message.REQUEST.value, b"piece": index})
    return struct.pack(
        f">LBB{len(payload)}s", 1 + 1 + len(payload), 20, ut_metadata, payload,
    )
