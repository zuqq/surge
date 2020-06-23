from __future__ import annotations
from typing import DefaultDict, Dict, List, Optional, Set

import asyncio
import collections
import functools
import random

from . import events
from . import messages
from . import metadata
from . import tracker
from .actor import Actor
from .stream import Stream


async def download(
        meta: metadata.Metadata,
        params: tracker.Parameters,
        missing: Set[metadata.Piece]):
    async with Root(meta, params, missing) as root:
        total = len(meta.pieces)
        progress = "\r\x1b[KProgress: {{:{}}}/{} pieces.".format(len(str(total)), total)
        chunks = metadata.piece_to_chunks(meta.files, meta.pieces)
        run_in_executor = asyncio.get_running_loop().run_in_executor
        write_chunk = functools.partial(metadata.write_chunk, meta.folder)
        # Print once before the loop to indicate that the download is starting.
        print(progress.format(total - len(missing)), end="")
        async for piece, data in root:
            for chunk in chunks[piece]:
                await run_in_executor(None, functools.partial(write_chunk, chunk, data))
                print(progress.format(total - len(missing)), end="")
        print("\n", end="")


class Root(Actor):
    def __init__(
            self,
            meta: metadata.Metadata,
            params: tracker.Parameters,
            missing: Set[metadata.Piece]):
        super().__init__()

        peer_queue = tracker.PeerQueue(self, meta.announce_list, params)
        self.children.add(peer_queue)
        self._coros.add(self._main(meta, params, peer_queue))

        self._missing = missing
        self._borrowers: DefaultDict[metadata.Piece, Set[Node]]
        self._borrowers = collections.defaultdict(set)

        self._slots = asyncio.Semaphore(50)
        self._queue = asyncio.Queue(10)  # type: ignore

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._queue.get()
        if item is None:
            raise StopAsyncIteration
        return item

    async def _main(self, meta, params, peer_queue):
        while True:
            await self._slots.acquire()
            peer = await peer_queue.get()
            await self.spawn_child(Node(self, meta, params, peer))

    def _on_child_crash(self, child):
        if isinstance(child, Node):
            for piece, borrowers in list(self._borrowers.items()):
                borrowers.discard(child)
                if not borrowers:
                    self._borrowers.pop(piece)
            self._slots.release()
        else:
            super()._on_child_crash(child)

    # Messages from `Node`.

    def get_piece(self, node: Node) -> Optional[metadata.Piece]:
        borrowed = set(self._borrowers)
        # Strict endgame mode: only send duplicate requests if every missing
        # piece is already being requested from some peer.
        pool = self._missing - borrowed or borrowed
        if not pool:
            return None
        try:
            piece = random.choice(list(pool & node.available))
        except IndexError:
            piece = random.choice(list(pool))
        self._borrowers[piece].add(node)
        return piece

    def return_piece(self, node: Node, piece: metadata.Piece):
        if piece not in self._borrowers:
            return
        self._borrowers[piece].remove(node)

    async def put_piece(self, node: Node, piece: metadata.Piece, data: bytes):
        if piece not in self._borrowers:
            return
        for borrower in self._borrowers.pop(piece) - {node}:
            borrower.cancel_piece(piece)
        self._queue.put_nowait((piece, data))
        self._missing.remove(piece)
        if not self._missing:
            self._queue.put_nowait(None)


class Progress:
    def __init__(self, piece, blocks):
        self._missing = set(blocks)
        self._data = bytearray(piece.length)

    @property
    def done(self):
        return not self._missing

    @property
    def data(self):
        return bytes(self._data)

    def add(self, block, data):
        self._missing.discard(block)
        self._data[block.begin : block.begin + block.length] = data


class Node(Actor):
    def __init__(
            self,
            parent: Root,
            meta: metadata.Metadata,
            params: tracker.Parameters,
            peer: tracker.Peer):
        super().__init__(parent)

        self._coros.add(self._main(meta.pieces, params.info_hash, params.peer_id))

        self.peer = peer
        self.available: Set[metadata.Block] = set()

        self._stack: List[metadata.Block] = []
        self._progress: Dict[metadata.Piece, Progress] = {}

    def _reset(self):
        self._stack = []
        for piece in self._progress:
            self.parent.return_piece(self, piece)
        self._progress = {}

    def _get_block(self) -> Optional[metadata.Block]:
        if not self._stack:
            piece = self.parent.get_piece(self)
            if piece is None:
                return None
            blocks = metadata.blocks(piece)
            self._stack = blocks[::-1]
            self._progress[piece] = Progress(piece, blocks)
        return self._stack.pop()

    async def _put_block(self, block: metadata.Block, data: bytes):
        piece = block.piece
        if piece not in self._progress:
            return
        progress = self._progress[piece]
        progress.add(block, data)
        if progress.done:
            data = self._progress.pop(piece).data
            if metadata.valid_piece(piece, data):
                await self.parent.put_piece(self, piece, data)
            else:
                raise ValueError("Peer sent invalid data.")

    async def _main(self, pieces, info_hash, peer_id):
        async with Stream(self.peer) as stream:
            transducer = events.Transducer(info_hash, peer_id)
            slots = 10

            # Unroll the first two iterations of the loop because handshake
            # messages don't have a length prefix.
            event = next(transducer)
            await stream.write(event.message)

            event = next(transducer)
            transducer.feed(await stream.read_handshake())

            for event in transducer:
                if isinstance(event, events.Receive):
                    message = event.message
                    if isinstance(message, messages.Choke):
                        self._reset()
                    elif isinstance(message, messages.Have):
                        self.available.add(message.piece(pieces))
                    elif isinstance(message, messages.Bitfield):
                        self.available = message.available(pieces)
                    elif isinstance(message, messages.Block):
                        await self._put_block(message.block(pieces), message.data)
                        slots += 1
                    elif isinstance(message, messages.Handshake):
                        if message.info_hash != info_hash:
                            raise ConnectionError("Peer's info_hash doesn't match.")
                elif isinstance(event, events.Send):
                    await stream.write(event.message)
                elif slots and isinstance(event, events.Request):
                    if (block := self._get_block()) is None:
                        break
                    await stream.write(messages.Request(block))
                    slots -= 1
                else:
                    transducer.feed(await stream.read_message())

    # Messages from `Root`.

    def cancel_piece(self, piece: metadata.Piece):
        if piece not in self._progress:
            return
        if self._stack and self._stack[-1].piece == piece:
            self._stack = []
        self._progress.pop(piece)
