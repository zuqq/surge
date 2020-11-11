"""Actors for the main protocol.

Every torrent download has an actor tree that is grounded in a central `Root`
instance. That `Root` has two kinds of children: a `surge.tracker.PeerQueue`
instance that is responsible for the tracker connections and a fixed number of
`Node` instances that connect to peers.

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
import functools
import random

from . import _metadata
from . import _transducer
from . import tracker
from .actor import Actor
from .channel import Channel
from .stream import Stream


async def print_progress(root: Root) -> None:
    """Periodically poll `root` and print the download progress to stdout."""
    progress_template = "\r\x1b[KDownload progress: {{}}/{} pieces".format(root.total)
    connections_template = "({} tracker{}, {} peer{})"
    try:
        while True:
            print(
                progress_template.format(root.total - root.missing),
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
            # Print one last time, so that the terminal output reflects the
            # final state.
            print(
                progress_template.format(root.total - root.missing),
                end=".\n",
                flush=True,
            )
        raise


async def download(metadata: _metadata.Metadata,
                   peer_id: bytes,
                   missing: Set[_metadata.Piece],
                   max_peers: int,
                   max_requests: int) -> None:
    """Spin up a `Root` and write downloaded pieces to the file system."""
    async with Root(metadata, peer_id, missing, max_peers, max_requests) as root:
        printer = asyncio.create_task(print_progress(root))
        chunks = _metadata.chunk(metadata.pieces, metadata.files)
        loop = asyncio.get_running_loop()
        # Delegate to a thread pool because asyncio has no direct support for
        # asynchronous file system operations.
        await loop.run_in_executor(
            None, functools.partial(_metadata.build_file_tree, metadata.files)
        )
        async for piece, data in root.results:
            for chunk in chunks[piece]:
                await loop.run_in_executor(
                    None, functools.partial(_metadata.write, chunk, data)
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

    def __init__(self,
                 metadata: _metadata.Metadata,
                 peer_id: bytes,
                 missing: Set[_metadata.Piece],
                 max_peers: int,
                 max_requests: int):
        super().__init__()
        self._peer_queue = tracker.PeerQueue(
            self, metadata.info_hash, metadata.announce_list, peer_id
        )
        self.children.add(self._peer_queue)
        self._coros.add(
            self._start_children(
                metadata.pieces,
                metadata.info_hash,
                peer_id,
                max_requests
            )
        )
        self._crashes = asyncio.Queue()  # type: ignore
        self._coros.add(self._stop_children())

        self._pieces = metadata.pieces
        # The set of pieces that still need to be downloaded.
        self._missing = missing
        # We connect to at most `max_peers` peers at the same time. This
        # semaphore is a means of communication between the coroutine that
        # cleans up after crashed `Node`s and the coroutine that spawns new
        # ones.
        self._slots = asyncio.Semaphore(max_peers)
        # In endgame mode, multiple `Node`s can be downloading the same piece;
        # as soon as one finishes downloading that piece, the other `Node`s
        # need to be notified. Therefore we keep track of which `Node`s are
        # downloading any given piece.
        self._downloading: DefaultDict[_metadata.Piece, Set[Node]]
        self._downloading = collections.defaultdict(set)

        # A `Channel` that holds up to `max_peers` downloaded pieces. If the
        # channel fills up, `Node`s will hold off on downloading more pieces
        # until the file system has caught up.
        self.results = Channel(max_peers)

    async def _start_children(self, pieces, info_hash, peer_id, max_requests):
        # In case `put` is never called because there are no pieces to download.
        if not self._missing:
            return await self.results.close()
        while True:
            await self._slots.acquire()
            peer = await self._peer_queue.get()
            await self.spawn_child(
                Node(self, pieces, info_hash, peer_id, peer, max_requests)
            )

    async def _stop_children(self):
        while True:
            child = await self._crashes.get()
            await child.stop()
            self.children.remove(child)
            for piece in child.downloading:
                self._downloading[piece].remove(child)
                if not self._downloading[piece]:
                    self._downloading.pop(piece)
            self._slots.release()

    def report_crash(self, child: Actor) -> None:
        if isinstance(child, Node):
            self._crashes.put_nowait(child)
        else:
            super().report_crash(child)

    @property
    def total(self) -> int:
        """The total number of pieces."""
        return len(self._pieces)

    @property
    def missing(self) -> int:
        """The number of missing pieces."""
        return len(self._missing)

    @property
    def trackers(self) -> int:
        """The number of connected trackers."""
        return self._peer_queue.trackers

    @property
    def peers(self) -> int:
        """The number of connected peers."""
        # Subtract `1` for the `PeerQueue`.
        return max(0, len(self.children) - 1)

    def get_nowait(self, node: Node) -> Optional[_metadata.Piece]:
        """Return a fresh piece to download.

        If there are no more pieces to download, return `None`.
        """
        downloading = set(self._downloading)
        # Strict endgame mode: only send duplicate requests if every missing
        # piece is already being requested from some peer.
        pool = (self._missing - downloading or downloading) - node.downloading
        if not pool:
            return None
        piece = random.choice(tuple(pool & node.available or pool))
        self._downloading[piece].add(node)
        return piece

    async def put(self, node: Node, piece: _metadata.Piece, data: bytes) -> None:
        """Deliver a downloaded piece.

        If any other nodes are in the process of downloading `piece`, those
        downloads are canceled.
        """
        if piece not in self._downloading:
            return
        self._missing.remove(piece)
        self._downloading[piece].remove(node)
        for child in self._downloading.pop(piece):
            child.cancel_piece(piece)
        await self.results.put((piece, data))
        if not self._missing:
            await self.results.close()


class Node(Actor):
    """Download pieces from a single peer.

    The `Node` crashes if the peer stops responding or sends invalid data.
    """

    def __init__(self,
                 parent: Root,
                 pieces: Sequence[_metadata.Piece],
                 info_hash: bytes,
                 peer_id: bytes,
                 peer: tracker.Peer,
                 max_requests: int):
        super().__init__(parent)
        self._coros.add(self._main(pieces, info_hash, peer_id))

        self._state = _transducer.State(max_requests)

        self.downloading: Set[_metadata.Piece] = set()
        self.peer = peer

    async def _main(self, pieces, info_hash, peer_id):
        async with Stream(self.peer) as stream:
            transducer = _transducer.base(pieces, info_hash, peer_id, self._state)
            message = None
            # Main loop that drives the transducer. See the documentation of the
            # `_transducer` module for more information.
            while True:
                event = transducer.send(message)
                message = None
                if isinstance(event, _transducer.Send):
                    await stream.write(event.message)
                elif isinstance(event, _transducer.PutPiece):
                    self.downloading.remove(event.piece)
                    await self.parent.put(self, event.piece, event.data)
                elif isinstance(event, _transducer.GetPiece):
                    if (piece := self.parent.get_nowait(self)) is None:
                        self._state.requesting = False
                    else:
                        self.downloading.add(piece)
                        self._state.add_piece(piece)
                elif isinstance(event, _transducer.ReceiveHandshake):
                    message = await asyncio.wait_for(stream.read_handshake(), 30)
                elif isinstance(event, _transducer.ReceiveMessage):
                    message = await asyncio.wait_for(stream.read(), 30)

    @property
    def available(self) -> Set[_metadata.Piece]:
        """The pieces that the peer advertises."""
        return self._state.available

    def cancel_piece(self, piece: _metadata.Piece) -> None:
        """Stop downloading `piece`."""
        self._state.cancel_piece(piece)
