from __future__ import annotations
from typing import DefaultDict, Optional, Sequence, Set

import asyncio
import collections
import contextlib
import dataclasses
import functools
import random

from . import _transducer
from . import metadata
from . import tracker
from .actor import Actor
from .channel import Channel
from .stream import Stream


async def print_progress(pieces: Sequence[metadata.Piece], root: Root):
    total = len(pieces)
    progress_template = "\r\x1b[KProgress: {{}}/{} pieces".format(total)
    connections_template = "({} tracker{}, {} peer{})"
    try:
        while True:
            print(
                progress_template.format(total - len(root.missing)),
                connections_template.format(
                    root.trackers,
                    "s" if root.trackers != 1 else "",
                    root.peers,
                    "s" if root.peers != 1 else "",
                ),
                end=".",
                flush=True,
            )
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        if not root.missing:
            # Print one last time, so that the final terminal output reflects
            # the fact that the download completed.
            print(progress_template.format(total), end=".\n", flush=True)
        raise


async def download(
    meta: metadata.Metadata,
    params: tracker.Parameters,
    missing: Set[metadata.Piece],
    max_peers: int,
    max_requests: int,
):
    async with Root(meta, params, missing, max_peers, max_requests) as root:
        printer = asyncio.create_task(print_progress(meta.pieces, root))
        chunks = metadata.piece_to_chunks(meta.pieces, meta.files)
        loop = asyncio.get_running_loop()
        folder = meta.folder
        await loop.run_in_executor(
            None, functools.partial(metadata.build_file_tree, folder, meta.files)
        )
        async for piece, data in root.results:
            for chunk in chunks[piece]:
                await loop.run_in_executor(
                    None, functools.partial(metadata.write_chunk, folder, chunk, data)
                )
        printer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await printer


class Root(Actor):
    def __init__(
        self,
        meta: metadata.Metadata,
        params: tracker.Parameters,
        missing: Set[metadata.Piece],
        max_peers: int,
        max_requests: int,
    ):
        super().__init__()
        self._peer_queue = tracker.PeerQueue(self, meta.announce_list, params)
        self.children.add(self._peer_queue)
        self._coros.add(
            self._main(meta.pieces, params.info_hash, params.peer_id, max_requests)
        )

        self.missing = missing
        self.results = Channel(max_peers)
        self._slots = asyncio.Semaphore(max_peers)
        self._downloading: DefaultDict[metadata.Piece, Set[Node]]
        self._downloading = collections.defaultdict(set)

    async def _main(self, pieces, info_hash, peer_id, max_requests):
        while True:
            await self._slots.acquire()
            peer = await self._peer_queue.get()
            await self.spawn_child(
                Node(self, pieces, info_hash, peer_id, peer, max_requests)
            )

    def _on_child_crash(self, child):
        if isinstance(child, Node):
            for piece in child.downloading:
                self._downloading[piece].remove(child)
                if not self._downloading[piece]:
                    self._downloading.pop(piece)
            self._slots.release()
        else:
            super()._on_child_crash(child)

    @property
    def trackers(self) -> int:
        return len(self._peer_queue.children)

    @property
    def peers(self) -> int:
        return max(0, len(self.children) - 1)

    def get_nowait(self, node: Node) -> Optional[metadata.Piece]:
        downloading = set(self._downloading)
        # Strict endgame mode: only send duplicate requests if every missing
        # piece is already being requested from some peer.
        pool = (self.missing - downloading or downloading) - node.downloading
        if not pool:
            return None
        piece = random.choice(tuple(pool & node.available or pool))
        self._downloading[piece].add(node)
        return piece

    async def put(self, node: Node, piece: metadata.Piece, data: bytes):
        if piece not in self._downloading:
            return
        self.missing.remove(piece)
        self._downloading[piece].remove(node)
        for child in self._downloading.pop(piece):
            child.cancel_piece(piece)
        await self.results.put((piece, data))
        if not self.missing:
            await self.results.close()


class Node(Actor):
    def __init__(
        self,
        parent: Root,
        pieces: Sequence[metadata.Piece],
        info_hash: bytes,
        peer_id: bytes,
        peer: tracker.Peer,
        max_requests: int,
    ):
        super().__init__(parent)
        self._coros.add(self._main(pieces, info_hash, peer_id))

        self.peer = peer
        self.downloading: Set[metadata.Piece] = set()
        self._state = _transducer.State(max_requests)

    def __repr__(self):
        class_name = self.__module__ + "." + self.__class__.__qualname__
        peer = dataclasses.astuple(self.peer)
        return f"<{class_name} with peer={peer}>"

    async def _main(self, pieces, info_hash, peer_id):
        async with Stream(self.peer) as stream:
            transducer = _transducer.base(pieces, info_hash, peer_id, self._state)
            message = None
            while True:
                event = transducer.send(message)
                message = None
                if isinstance(event, _transducer.Write):
                    await stream.write(event.message)
                elif isinstance(event, _transducer.Result):
                    self.downloading.remove(event.piece)
                    await self.parent.put(self, event.piece, event.data)
                elif isinstance(event, _transducer.NeedPiece):
                    if (piece := self.parent.get_nowait(self)) is None:
                        self._state.requesting = False
                    else:
                        self.downloading.add(piece)
                        self._state.add_piece(piece)
                elif isinstance(event, _transducer.NeedHandshake):
                    message = await asyncio.wait_for(stream.read_handshake(), 30)
                elif isinstance(event, _transducer.NeedMessage):
                    message = await asyncio.wait_for(stream.read(), 30)

    @property
    def available(self):
        return self._state.available

    def cancel_piece(self, piece: metadata.Piece):
        self._state.cancel_piece(piece)
