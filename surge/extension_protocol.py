from typing import Dict

import enum
import struct

from . import bencoding


class Message(enum.Enum):
    """Map extension protocol message types to their identifiers."""

    HANDSHAKE = 0
    UT_METADATA = 3


def message_type(message: bytes) -> Message:
    return Message(message[0])


def handshake() -> bytes:
    payload = bencoding.encode({b"m": {b"ut_metadata": Message.UT_METADATA.value}})
    return struct.pack(f">LBB{len(payload)}s", 1 + 1 + len(payload), 20, 0, payload)
