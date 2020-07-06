from typing import Dict, Iterable, List, Optional, Sequence, Set, Union

import dataclasses

from . import messages
from . import metadata


# Events -----------------------------------------------------------------------


@dataclasses.dataclass
class Receive:
    message: messages.Message


@dataclasses.dataclass
class Send:
    message: messages.Message


class Request:
    pass


@dataclasses.dataclass
class Result:
    piece: metadata.Piece
    data: bytes


class NeedPiece:
    pass


class NeedMessage:
    pass


# States -----------------------------------------------------------------------


class _State:
    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return self._name


_CLOSED = _State("CLOSED")
_OPEN = _State("OPEN")
_WAITING = _State("WAITING")
_CHOKED = _State("CHOKED")
_INTERESTED = _State("INTERESTED")
_UNCHOKED = _State("UNCHOKED")


# Transducer -------------------------------------------------------------------


class Transducer:
    def __init__(
            self,
            pieces: Sequence[metadata.Piece],
            info_hash: bytes,
            peer_id: bytes):
        self.available: Set[metadata.Piece] = set()  # Pieces that the peer has.
        self._pieces = pieces
        self._info_hash = info_hash
        self._peer_id = peer_id
        self._state = _CLOSED

        # Maps `(state, message_type)` to `(callback, relay, new_state)`, where
        # `relay` is a boolean indicating whether or not to relay the message to
        # the caller. If a pair `(state, message_type)` does not appear here,
        # then it is a no-op.
        self._receive = {
            (_UNCHOKED, messages.Choke): (None, True, _CHOKED),
            (_CHOKED, messages.Unchoke): (None, False, _UNCHOKED),
            (_INTERESTED, messages.Unchoke): (None, False, _UNCHOKED),
            (_CHOKED, messages.Have): (self._on_have, False, _CHOKED),
            (_UNCHOKED, messages.Have): (self._on_have, False, _UNCHOKED),
            (_WAITING, messages.Bitfield): (self._on_bitfield, False, _CHOKED),
            (_CHOKED, messages.Block): (None, True, _CHOKED),
            (_UNCHOKED, messages.Block): (None, True, _UNCHOKED),
            (_OPEN, messages.Handshake): (self._on_handshake, False, _WAITING),
        }

    def _on_have(self, message):
        self.available.add(message.piece(self._pieces))

    def _on_bitfield(self, message):
        self.available = message.available(self._pieces)

    def _on_handshake(self, message):
        if message.info_hash != self._info_hash:
            raise ConnectionError("Peer's info_hash doesn't match.")

    def send(self, message) -> Union[Send, Receive, Request, NeedMessage]:
        if message is not None:
            callback, relay, self._state = self._receive.get(
                (self._state, type(message)), (None, False, self._state)
            )
            if callback is not None:
                callback(message)
            if relay:
                return Receive(message)

        if self._state is _CLOSED:
            self._state = _OPEN
            return Send(messages.Handshake(self._info_hash, self._peer_id))

        if self._state is _CHOKED:
            self._state = _INTERESTED
            return Send(messages.Interested())

        if self._state is _UNCHOKED:
            return Request()

        return NeedMessage()


# Wrapper ----------------------------------------------------------------------


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


class State(Transducer):
    """A wrapper around `Transducer` that downloads pieces."""

    def __init__(
            self,
            pieces: Sequence[metadata.Piece],
            info_hash: bytes,
            peer_id: bytes,
            max_requests: int):
        super().__init__(pieces, info_hash, peer_id)

        self.should_request = True  # Are there more blocks to request?
        self._max_requests = max_requests
        self._progress: Dict[metadata.Piece, Progress] = {}
        self._stack: List[metadata.Block] = []
        self._requested: Set[metadata.Block] = set()

    def _on_choke(self):
        in_progress = tuple(self._progress)
        self._progress.clear()
        self._stack.clear()
        self._requested.clear()
        for piece in in_progress:
            self.send_piece(piece)

    def _on_block(self, message) -> Optional[Result]:
        block = message.block(self._pieces)
        if block not in self._requested:
            return None
        self._requested.remove(block)
        piece = block.piece
        progress = self._progress[piece]
        progress.add(block, message.data)
        if not progress.done:
            return None
        data = self._progress.pop(piece).data
        if metadata.valid_piece(piece, data):
            return Result(piece, data)
        raise ConnectionError("Invalid data.")

    def send(self, message) -> Union[Send, Result, NeedPiece, NeedMessage]:
        while True:
            event = super().send(message)
            message = None
            if isinstance(event, Send):
                return event
            if isinstance(event, Receive):
                received = event.message
                if isinstance(received, messages.Choke):
                    self._on_choke()
                elif isinstance(received, messages.Block):
                    result = self._on_block(received)
                    if result is not None:
                        return result
            elif (isinstance(event, Request)
                  and self.should_request
                  and len(self._requested) < self._max_requests):
                if not self._stack:
                    return NeedPiece()
                block = self._stack.pop()
                self._requested.add(block)
                return Send(messages.Request(block))
            else:
                return NeedMessage()

    def send_piece(self, piece: metadata.Piece):
        blocks = tuple(metadata.blocks(piece))
        self._progress[piece] = Progress(piece, blocks)
        self._stack.extend(reversed(blocks))

    def cancel_piece(self, piece: metadata.Piece):
        self._progress.pop(piece)
        self._requested = {block for block in self._requested if block.piece != piece}
        self._stack = [block for block in self._stack if block.piece != piece]
