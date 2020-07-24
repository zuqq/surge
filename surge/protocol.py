from typing import Dict, Iterable, List, Optional, Sequence, Set, Union

import dataclasses
import enum

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


# Transducer -------------------------------------------------------------------


class ConnectionState(enum.IntEnum):
    CHOKED = 0
    INTERESTED = 1
    UNCHOKED = 2


def base(pieces, info_hash, peer_id, available):
    received = yield Send(messages.Handshake(info_hash, peer_id))
    received = yield NeedMessage()
    if not isinstance(received, messages.Handshake):
        raise TypeError("Expected handshake.")
    if received.info_hash != info_hash:
        raise ValueError("Wrong 'info_hash'.")

    # Wait for the peer to send us its bitfield. This is not mandated by the
    # specification, but makes requesting pieces later much easier.
    while True:
        received = yield NeedMessage()
        if isinstance(received, messages.Bitfield):
            available.update(received.available(pieces))
            break

    state = ConnectionState.CHOKED
    event = None
    while True:
        if event is None:
            if state is ConnectionState.CHOKED:
                state = ConnectionState.INTERESTED
                event = Send(messages.Interested())
            elif state is ConnectionState.INTERESTED:
                event = NeedMessage()
            elif state is ConnectionState.UNCHOKED:
                event = Request()

        received = yield event
        event = None

        if isinstance(received, messages.Choke):
            state = ConnectionState.CHOKED
            event = Receive(received)
        elif isinstance(received, messages.Unchoke):
            state = ConnectionState.UNCHOKED
        elif isinstance(received, messages.Have):
            available.add(received.piece(pieces))
        elif isinstance(received, messages.Block):
            event = Receive(received)


# DownloadState ----------------------------------------------------------------


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


class DownloadState:
    def __init__(self, pieces, max_requests):
        self.available: Set[metadata.Piece] = set()
        self.requesting = True  # Are there more blocks to request?
        self._pieces = pieces
        self._max_requests = max_requests
        self._progress: Dict[metadata.Piece, Progress] = {}
        self._stack: List[metadata.Block] = []
        self._requested: Set[metadata.Block] = set()

    @property
    def can_request(self):
        return self.requesting and len(self._requested) < self._max_requests

    def add_piece(self, piece):
        blocks = tuple(metadata.blocks(piece))
        self._progress[piece] = Progress(piece, blocks)
        self._stack.extend(reversed(blocks))

    def cancel_piece(self, piece):
        self._progress.pop(piece)
        self._requested = {b for b in self._requested if b.piece != piece}
        self._stack = [b for b in self._stack if b.piece != piece]

    def get_block(self):
        block = self._stack.pop()
        self._requested.add(block)
        return block

    def on_block(self, message):
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
        raise ValueError("Invalid data.")

    def on_choke(self):
        in_progress = tuple(self._progress)
        self._progress.clear()
        self._stack.clear()
        self._requested.clear()
        for piece in in_progress:
            self.add_piece(piece)


def next_event(state, transducer, message):
    while True:
        event = transducer.send(message)
        message = None
        if isinstance(event, Send):
            return event
        elif isinstance(event, Receive):
            received = event.message
            if isinstance(received, messages.Choke):
                state.on_choke()
            elif isinstance(received, messages.Block):
                result = state.on_block(received)
                if result is not None:
                    return result
        elif isinstance(event, Request) and state.can_request:
            try:
                block = state.get_block()
            except IndexError:
                return NeedPiece()
            return Send(messages.Request.from_block(block))
        else:
            return NeedMessage()
