"""State machine for the main protocol.

Instead of a state machine that does its own calls to I/O-performing methods,
this module provides a pure transducer that receives `messages.Message`s and
yields `Event`s. In particular, the transducer is completely agnostic as to
which kind of I/O is performed (or whether any is performed at all!).

Code that wants to run the transducer sends it `messages.Message`s and
dispatches on the type of `Event`s that the transducer yields; an `Event`
represents an action that the caller should take in order to advance the state
of the connection.
"""

from typing import Dict, Generator, Iterable, List, Optional, Sequence, Set, Union

import dataclasses
import enum

from . import messages
from . import metadata


@dataclasses.dataclass
class Send:
    """Send `message`."""

    message: messages.Message


@dataclasses.dataclass
class PutPiece:
    """Process the downloaded `data` for `piece`."""

    piece: metadata.Piece
    data: bytes


# This is separate from `ReceiveMessage` because the handshake message has
# a fixed length, unlike the rest of the messages which are length-prefixed.
class ReceiveHandshake:
    """Receive a `messages.Handshake`."""


class ReceiveMessage:
    """Receive a `messages.Message`."""


class GetPiece:
    """Call `State.add_piece` with a piece to download."""


Event = Union[Send, PutPiece, ReceiveHandshake, ReceiveMessage, GetPiece]


class Progress:
    """Helper class that keeps track of a single piece's progress."""

    def __init__(self, piece: metadata.Piece, blocks: Iterable[metadata.Block]):
        self._missing = set(blocks)
        self._data = bytearray(piece.length)

    @property
    def done(self) -> bool:
        return not self._missing

    @property
    def data(self) -> bytes:
        return bytes(self._data)

    def add(self, block: metadata.Block, data: bytes) -> None:
        self._missing.discard(block)
        self._data[block.begin : block.begin + block.length] = data


class State:
    """Connection state, especially the parts pertaining to open block request.

    This class is also used to synchronously pass information from the driver
    to the `base` generator, via the methods `add_piece` and `cancel_piece`.
    """

    def __init__(self, max_requests: int):
        self._max_requests = max_requests

        self._progress: Dict[metadata.Piece, Progress] = {}
        # Blocks to request next.
        self._stack: List[metadata.Block] = []
        self._requested: Set[metadata.Block] = set()

        # Pieces that the peer is advertising.
        self.available: Set[metadata.Piece] = set()
        self.requesting = True

    @property
    def can_request(self):
        return self.requesting and len(self._requested) < self._max_requests

    def add_piece(self, piece: metadata.Piece) -> None:
        """Add `piece` to the download queue."""
        blocks = tuple(metadata.blocks(piece))
        self._progress[piece] = Progress(piece, blocks)
        self._stack.extend(reversed(blocks))

    def cancel_piece(self, piece: metadata.Piece) -> None:
        """Cancel `piece`, discarding partial progress."""
        self._progress.pop(piece)

        def predicate(block):
            return block.piece != piece

        self._requested = set(filter(predicate, self._requested))
        self._stack = list(filter(predicate, self._stack))

    def get_block(self) -> metadata.Block:
        """"Return a fresh block to download.

        Raise `IndexError` if there are no blocks available.
        """
        block = self._stack.pop()
        self._requested.add(block)
        return block

    def on_block(self, block: metadata.Block, data: bytes) -> Optional[PutPiece]:
        """Deliver a downloaded block.

        If `block` was the last missing block of its piece, return a `Result`.
        """
        if block not in self._requested:
            return None
        self._requested.remove(block)
        piece = block.piece
        progress = self._progress[piece]
        progress.add(block, data)
        if not progress.done:
            return None
        data = self._progress.pop(piece).data
        if metadata.valid(piece, data):
            return PutPiece(piece, data)
        raise ValueError("Invalid data.")

    def on_choke(self) -> None:
        """Cancel all pieces, discarding partial progress."""
        in_progress = tuple(self._progress)
        self._progress.clear()
        self._stack.clear()
        self._requested.clear()
        for piece in in_progress:
            self.add_piece(piece)


class Flow(enum.IntEnum):
    """Additional state that indicates whether we are allowed to request.

    `CHOKED` is the initial state. We transition from `CHOKED` to `INTERESTED`
    by sending a `messages.Interested` message to the peer and then to
    `UNCHOKED` by receiving a `messages.Unchoke` message. The peer will only
    honor our `messages.Request` messages if we're `UNCHOKED`.
    """

    CHOKED = 0
    INTERESTED = 1
    UNCHOKED = 2


def base(pieces: Sequence[metadata.Piece],
         info_hash: bytes,
         peer_id: bytes,
         state: State) -> Generator[Event, Optional[messages.Message], None]:
    yield Send(messages.Handshake(info_hash, peer_id))
    received = yield ReceiveHandshake()
    if not isinstance(received, messages.Handshake):
        raise TypeError("Expected handshake.")
    if received.info_hash != info_hash:
        raise ValueError("Wrong 'info_hash'.")

    # Wait for the peer to tell us which pieces it has. This is not mandated by
    # the specification, but makes requesting pieces much easier.
    while True:
        received = yield ReceiveMessage()
        if isinstance(received, messages.Have):
            state.available.add(received.piece(pieces))
            break
        if isinstance(received, messages.Bitfield):
            state.available.update(received.available(pieces))
            break

    flow = Flow.CHOKED
    while True:
        event: Event = ReceiveMessage()
        if flow is Flow.CHOKED:
            flow = Flow.INTERESTED
            event = Send(messages.Interested())
        elif flow is Flow.UNCHOKED and state.can_request:
            try:
                block = state.get_block()
            except IndexError:
                event = GetPiece()
            else:
                event = Send(messages.Request.from_block(block))
        received = yield event
        if isinstance(received, messages.Choke):
            flow = Flow.CHOKED
            state.on_choke()
        elif isinstance(received, messages.Unchoke):
            flow = Flow.UNCHOKED
        elif isinstance(received, messages.Have):
            state.available.add(received.piece(pieces))
        elif isinstance(received, messages.Block):
            result = state.on_block(received.block(pieces), received.data)
            if result is not None:
                yield result
