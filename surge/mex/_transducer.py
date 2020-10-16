"""State machine for the metadata exchange protocol.

The structure is similar to that of the main protocol. Howeve, because this
protocol is much simpler, there is no need for a separate `State` object and
there are fewer types of `Event`s.
"""

from typing import Generator, Optional, Union

import dataclasses

from . import _info
from .. import messages


@dataclasses.dataclass
class Send:
    """Send `message`."""

    message: messages.Message


class ReceiveHandshake:
    """Receive a `messages.Handshake`."""


class ReceiveMessage:
    """Receive a `messages.Message`."""


Event = Union[Send, ReceiveHandshake, ReceiveMessage]


def mex(info_hash: bytes, peer_id: bytes) -> Generator[Event,
                                                       Optional[messages.Message],
                                                       bytes]:
    yield Send(messages.Handshake(1 << 20, info_hash, peer_id))
    received = yield ReceiveHandshake()
    if not isinstance(received, messages.Handshake):
        raise TypeError("Expected handshake.")
    if received.info_hash != info_hash:
        raise ValueError("Wrong 'info_hash'.")

    yield Send(messages.ExtensionHandshake())
    while True:
        received = yield ReceiveMessage()
        if isinstance(received, messages.ExtensionHandshake):
            ut_metadata = received.ut_metadata
            metadata_size = received.metadata_size
            break

    # The metadata is partitioned into pieces of size `2 ** 14`, except for the
    # last piece which may be smaller. The peer knows this partition, so we only
    # need to tell it the indices of the pieces that we want. Because the total
    # number of pieces is typically very small, a simple stop-and-wait protocol
    # is fast enough.
    piece_length = 2 ** 14
    pieces = []
    for i in range((metadata_size + piece_length - 1) // piece_length):  # type: ignore
        yield Send(messages.MetadataRequest(i, ut_metadata))
        while True:
            received = yield ReceiveMessage()
            if isinstance(received, messages.MetadataData):
                # We assume that the peer sends us data for the piece that we
                # just requested; if not, the result of the transaction will be
                # invalid. This assumption is reasonable because we request one
                # piece at a time.
                pieces.append(received.data)
                break
    raw_info = b"".join(pieces)
    if _info.valid(info_hash, raw_info):
        return raw_info
    raise ValueError("Invalid data.")
