"""Actors for the main protocol.

Every torrent download has an actor tree that is grounded in a central `Root`
instance. That `Root` has two kinds of children: a `PeerQueue` instance
controlling the tracker connections and a fixed number of `Node` instances that
connect to peers.

With respect to its `Node`s, the `Root` has three responsibilities: picking
pieces for them to download, replacing `Node`s that crash (the peer stopped
responding, sent invalid data, etc.), and collecting the downloaded pieces.

Downloaded pieces are exposed via `Root.results`; the `download` coroutine
retrieves them from there and writes them to the file system.
"""

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


async def print_progress(pieces: Sequence[metadata.Piece], root: Root) -> None:
    """Periodically poll `root` and print the download progress to stdout."""
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
            await asyncio.sleep(0.5)
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
) -> None:
    """Spin up a `Root` and write downloaded pieces to the file system."""
    async with Root(meta, params, missing, max_peers, max_requests) as root:
        printer = asyncio.create_task(print_progress(meta.pieces, root))
        chunks = metadata.piece_to_chunks(meta.pieces, meta.files)
        loop = asyncio.get_running_loop()
        folder = meta.folder
        # Delegate to a thread pool because asyncio has no direct support for
        # asynchronous file system operations.
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
    """Root of the actor tree.

    Children are spawned in `_main`, with crashes being handled by the `Actor`
    interface. In order to obtain pieces to download, `Node`s call `get_nowait`;
    downloaded pieces are reported via `put`.

    This class also exposes `missing`, `trackers`, and `peers`, which provide
    information about the download.
    """

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

        # The set of pieces that still need to be downloaded.
        self.missing = missing
        # A `Channel` that holds up to `max_peers` downloaded pieces. If the
        # channel fills up, `Node`s will hold off on downloading more pieces
        # until the file system has caught up.
        self.results = Channel(max_peers)
        # We connect to at most `max_peers` peers at the same time. This
        # semaphore is a means of communication between the coroutine that
        # cleans up after crashed `Node`s and the coroutine that spawns new
        # ones.
        self._slots = asyncio.Semaphore(max_peers)
        # In endgame mode, multiple `Node`s can be downloading the same piece;
        # as soon as one finishes downloading that piece, the other `Node`s
        # need to be notified. Therefore we keep track of which `Node`s are
        # downloading any given piece.
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
        """The number of connected trackers."""
        return len(self._peer_queue.children)

    @property
    def peers(self) -> int:
        """The number of connected peers."""
        return max(0, len(self.children) - 1)

    def get_nowait(self, node: Node) -> Optional[metadata.Piece]:
        """Return a fresh piece to download.

        If there are no more pieces to download, return `None`.
        """
        downloading = set(self._downloading)
        # Strict endgame mode: only send duplicate requests if every missing
        # piece is already being requested from some peer.
        pool = (self.missing - downloading or downloading) - node.downloading
        if not pool:
            return None
        piece = random.choice(tuple(pool & node.available or pool))
        self._downloading[piece].add(node)
        return piece

    async def put(self, node: Node, piece: metadata.Piece, data: bytes) -> None:
        """Deliver a downloaded piece.

        If any other nodes are in the process of downloading `piece`, those
        downloads are canceled.
        """
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
    """Download pieces from a single peer.

    The `Node` crashes if the peer stops responding or sends invalid data.
    """

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
            # Main loop that drives the transducer. See the documentation of the
            # `_transducer` module for more information.
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
    def available(self) -> Set[metadata.Piece]:
        """The pieces that the peer advertises."""
        return self._state.available

    def cancel_piece(self, piece: metadata.Piece) -> None:
        """Stop downloading `piece`."""
        self._state.cancel_piece(piece)
