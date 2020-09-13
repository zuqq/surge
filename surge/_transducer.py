from typing import Dict, Generator, Iterable, List, Optional, Sequence, Set, Union

import dataclasses
import enum

from . import messages
from . import metadata


@dataclasses.dataclass
class Write:
    message: messages.Message


@dataclasses.dataclass
class Result:
    piece: metadata.Piece
    data: bytes


class NeedHandshake:
    pass


class NeedMessage:
    pass


class NeedPiece:
    pass


Event = Union[Write, Result, NeedHandshake, NeedMessage, NeedPiece]


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

    def add(self, block: metadata.Block, data: bytes):
        self._missing.discard(block)
        self._data[block.begin : block.begin + block.length] = data


class State:
    def __init__(self, max_requests: int):
        self._max_requests = max_requests

        self._progress: Dict[metadata.Piece, Progress] = {}
        self._stack: List[metadata.Block] = []
        self._requested: Set[metadata.Block] = set()

        self.available: Set[metadata.Piece] = set()
        self.requesting = True

    @property
    def can_request(self):
        return self.requesting and len(self._requested) < self._max_requests

    def add_piece(self, piece: metadata.Piece):
        blocks = tuple(metadata.blocks(piece))
        self._progress[piece] = Progress(piece, blocks)
        self._stack.extend(reversed(blocks))

    def cancel_piece(self, piece: metadata.Piece):
        self._progress.pop(piece)

        def predicate(block):
            return block.piece != piece

        self._requested = set(filter(predicate, self._requested))
        self._stack = list(filter(predicate, self._stack))

    def get_block(self):
        """"Return a fresh block.

        Raise `IndexError` if there are no blocks available.
        """
        block = self._stack.pop()
        self._requested.add(block)
        return block

    def on_block(self, block: metadata.Block, data: bytes) -> Optional[Result]:
        if block not in self._requested:
            return None
        self._requested.remove(block)
        piece = block.piece
        progress = self._progress[piece]
        progress.add(block, data)
        if not progress.done:
            return None
        data = self._progress.pop(piece).data
        if metadata.valid_piece(piece, data):
            return Result(piece, data)
        raise ValueError("Invalid data.")

    def on_choke(self):
        in_progress = tuple(self._progress)
        self._progress.clear()
        self._stack.clear()
        self._requested.clear()
        for piece in in_progress:
            self.add_piece(piece)


class Flow(enum.IntEnum):
    """Additional state that indicates whether we are allowed to request."""

    CHOKED = 0
    INTERESTED = 1
    UNCHOKED = 2


def base(
    pieces: Sequence[metadata.Piece], info_hash: bytes, peer_id: bytes, state: State
) -> Generator[Event, Optional[messages.Message], None]:
    yield Write(messages.Handshake(info_hash, peer_id))
    received = yield NeedHandshake()
    if not isinstance(received, messages.Handshake):
        raise TypeError("Expected handshake.")
    if received.info_hash != info_hash:
        raise ValueError("Wrong 'info_hash'.")

    # Wait for the peer to tell us which pieces it has. This is not mandated by
    # the specification, but makes requesting pieces much easier.
    while True:
        received = yield NeedMessage()
        if isinstance(received, messages.Have):
            state.available.add(received.piece(pieces))
            break
        if isinstance(received, messages.Bitfield):
            state.available.update(received.available(pieces))
            break

    flow = Flow.CHOKED
    while True:
        event: Event = NeedMessage()
        if flow is Flow.CHOKED:
            flow = Flow.INTERESTED
            event = Write(messages.Interested())
        elif flow is Flow.UNCHOKED and state.can_request:
            try:
                block = state.get_block()
            except IndexError:
                event = NeedPiece()
            else:
                event = Write(messages.Request.from_block(block))
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
