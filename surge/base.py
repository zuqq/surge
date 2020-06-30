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

        self.missing = missing
        self._queue = asyncio.Queue(10)  # type: ignore
        self._slots = asyncio.Semaphore(50)
        self._downloading: DefaultDict[metadata.Piece, Set[Node]]
        self._downloading = collections.defaultdict(set)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if (item := await self._queue.get()) is None:
            raise StopAsyncIteration
        return item

    async def _main(self, meta, params, peer_queue):
        while True:
            await self._slots.acquire()
            peer = await peer_queue.get()
            await self.spawn_child(Node(self, meta, params, peer))

    def _on_child_crash(self, child):
        if isinstance(child, Node):
            for piece in child.downloading:
                self._downloading[piece].remove(child)
                if not self._downloading[piece]:
                    self._downloading.pop(piece)
            self._slots.release()
        else:
            super()._on_child_crash(child)

    def get_piece(self, node: Node) -> Optional[metadata.Piece]:
        downloading = set(self._downloading)
        # Strict endgame mode: only send duplicate requests if every missing
        # piece is already being requested from some peer.
        pool = (self.missing - downloading or downloading) - node.downloading
        if not pool:
            return None
        piece = random.choice(tuple(pool & node.available or pool))
        self._downloading[piece].add(node)
        return piece

    async def put_piece(self, node: Node, piece: metadata.Piece, data: bytes):
        if piece not in self._downloading:
            return
        self.missing.remove(piece)
        self._downloading[piece].remove(node)
        for child in self._downloading.pop(piece):
            child.cancel_piece(piece)
        await self._queue.put((piece, data))
        if not self.missing:
            await self._queue.put(None)


class Node(Actor):
    def __init__(
            self,
            parent: Root,
            meta: metadata.Metadata,
            params: tracker.Parameters,
            peer: tracker.Peer):
        super().__init__(parent)
        self._coros.add(self._main())

        self.peer = peer
        self.downloading: Set[metadata.Piece] = set()
        self._state = events.Wrapper(meta.pieces, params.info_hash, params.peer_id)

    async def _main(self):
        async with Stream(self.peer) as stream:
            state = self._state

            # Unroll the first two iterations of the loop because handshake
            # messages don't have a length prefix.
            event = state.send(None)
            await stream.write(event.message)

            event = state.send(None)
            message = await stream.read_handshake()

            while True:
                event = state.send(message)
                message = None
                if isinstance(event, events.Send):
                    await stream.write(event.message)
                elif isinstance(event, events.Result):
                    self.downloading.remove(event.piece)
                    await self.parent.put_piece(self, event.piece, event.data)
                elif isinstance(event, events.NeedPiece):
                    piece = self.parent.get_piece(self)
                    self.downloading.add(piece)
                    state.download_piece(piece)
                elif isinstance(event, events.NeedMessage):
                    message = await asyncio.wait_for(stream.read_message(), 5)

    @property
    def available(self):
        return self._state.available

    def cancel_piece(self, piece: metadata.Piece):
        self._state.cancel_piece(piece)
