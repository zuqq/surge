import asyncio
import collections
import enum
import functools
import os
import random

from . import _metadata
from . import messages
from . import tracker
from .channel import Channel
from .stream import open_stream


class State(enum.IntEnum):
    """Connection state after handshakes are exchanged."""

    CHOKED = enum.auto()
    INTERESTED = enum.auto()
    UNCHOKED = enum.auto()
    # There are no more blocks to request.
    PASSIVE = enum.auto()


class Progress:
    """A single piece's progress."""

    def __init__(self, piece, blocks):
        self._missing_blocks = set(blocks)
        self._data = bytearray(piece.length)

    @property
    def done(self):
        return not self._missing_blocks

    @property
    def data(self):
        return bytes(self._data)

    def add(self, block, data):
        self._missing_blocks.discard(block)
        self._data[block.begin : block.begin + block.length] = data


class Queue:
    """Download queue."""

    def __init__(self):
        self._progress = {}
        self._requested = set()
        self._queue = collections.deque()

    @property
    def requested(self):
        return len(self._requested)

    def add_piece(self, piece):
        """Add `piece` to the download queue."""
        blocks = set(_metadata.yield_blocks(piece))
        self._progress[piece] = Progress(piece, blocks)
        self._queue.extendleft(blocks)

    def reset_progress(self):
        """Reset the progress of all pieces in the download queue."""
        in_progress = tuple(self._progress)
        self._progress.clear()
        self._requested.clear()
        self._queue.clear()
        for piece in in_progress:
            self.add_piece(piece)

    def get_block(self):
        """Return a block to download next.

        Raise `IndexError` if the block queue is empty.
        """
        block = self._queue.pop()
        self._requested.add(block)
        return block

    def put_block(self, block, data):
        """Deliver a downloaded block.

        Return the piece and its data if this block completes its piece.
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
        if _metadata.valid_piece_data(piece, data):
            return (piece, data)
        raise ValueError("Invalid data.")


async def download_from_peer(root, peer, info_hash, peer_id, pieces, max_requests):
    async with open_stream(peer) as stream:
        await stream.write(messages.Handshake(0, info_hash, peer_id))
        received = await stream.read_handshake()
        if received.info_hash != info_hash:
            raise ValueError("Wrong 'info_hash'.")
        available = set()
        # Wait for the peer to tell us which pieces it has. This is not mandated
        # by the specification, but makes requesting pieces much easier.
        while True:
            received = await stream.read()
            if isinstance(received, messages.Have):
                available.add(pieces[received.index])
                break
            if isinstance(received, messages.Bitfield):
                for i in received.to_indices():
                    available.add(pieces[i])
                break
        state = State.CHOKED
        queue = Queue()
        while True:
            if state is State.CHOKED:
                await stream.write(messages.Interested())
                state = State.INTERESTED
            elif state is State.UNCHOKED and queue.requested < max_requests:
                try:
                    block = queue.get_block()
                except IndexError:
                    try:
                        piece = root.get_piece(peer, available)
                    except IndexError:
                        state = State.PASSIVE
                    else:
                        queue.add_piece(piece)
                else:
                    await stream.write(messages.Request.from_block(block))
            else:
                received = await stream.read()
                if isinstance(received, messages.Choke):
                    queue.reset_progress()
                    state = State.CHOKED
                elif isinstance(received, messages.Unchoke):
                    if state is not State.PASSIVE:
                        state = State.UNCHOKED
                elif isinstance(received, messages.Have):
                    available.add(pieces[received.index])
                    if state is State.PASSIVE:
                        state = State.UNCHOKED
                elif isinstance(received, messages.Block):
                    result = queue.put_block(
                        _metadata.Block(
                            pieces[received.index],
                            received.begin,
                            len(received.data),
                        ),
                        received.data,
                    )
                    if result is not None:
                        await root.put_piece(peer, *result)


async def download_from_peer_loop(root, info_hash, peer_id, pieces, max_requests):
    while True:
        peer = await root.get_peer()
        try:
            root.peer_connected(peer)
            await download_from_peer(
                root, peer, info_hash, peer_id, pieces, max_requests
            )
        except Exception:
            pass
        finally:
            root.peer_disconnected(peer)


class Root(tracker.TrackerMixin):
    def __init__(
        self,
        info_hash,
        peer_id,
        announce_list,
        pieces,
        missing_pieces,
        max_peers,
        max_requests,
    ):
        super().__init__(info_hash, peer_id, announce_list, max_peers)

        self.info_hash = info_hash
        self.peer_id = peer_id
        self.pieces = pieces
        self.max_peers = max_peers
        self.max_requests = max_requests

        self.results = Channel(max_peers)

        self._missing_pieces = set(missing_pieces)
        self._peer_to_pieces = {}
        self._piece_to_peers = collections.defaultdict(set)

        self._tasks = set()

        # This check is necessary because `put_piece` is never called if there
        # are no pieces to download.
        if not self._missing_pieces:
            self.results.close_nowait()

    @property
    def missing_pieces(self):
        """The number of missing pieces."""
        return len(self._missing_pieces)

    @property
    def connected_peers(self):
        """The number of connected peers."""
        return len(self._peer_to_pieces)

    def get_piece(self, peer, available):
        """Return a piece to download next.

        Raise `IndexError` if there are no additional pieces to download.
        """
        pool = self._missing_pieces & (available - self._peer_to_pieces[peer])
        piece = random.choice(tuple(pool - set(self._piece_to_peers) or pool))
        self._peer_to_pieces[peer].add(piece)
        self._piece_to_peers[piece].add(peer)
        return piece

    async def put_piece(self, peer, piece, data):
        """Deliver a downloaded piece."""
        if piece not in self._missing_pieces:
            return
        self._missing_pieces.remove(piece)
        self._peer_to_pieces[peer].remove(piece)
        self._piece_to_peers[piece].remove(peer)
        if not self._piece_to_peers[piece]:
            self._piece_to_peers.pop(piece)
        await self.results.put((piece, data))
        if not self._missing_pieces:
            await self.results.close()

    def peer_connected(self, peer):
        self._peer_to_pieces[peer] = set()

    def peer_disconnected(self, peer):
        for piece in self._peer_to_pieces.pop(peer):
            self._piece_to_peers[piece].remove(peer)
            if not self._piece_to_peers[piece]:
                self._piece_to_peers.pop(piece)

    def start(self):
        super().start()
        for _ in range(self.max_peers):
            self._tasks.add(
                asyncio.create_task(
                    download_from_peer_loop(
                        self,
                        self.info_hash,
                        self.peer_id,
                        self.pieces,
                        self.max_requests,
                    )
                )
            )

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(super().stop(), *self._tasks, return_exceptions=True)
        self._tasks.clear()


async def print_progress(root):
    """Periodically poll `root` and print the download progress to stdout."""
    total = len(root.pieces)
    progress_template = "Download progress: {{}}/{} pieces".format(total)
    connections_template = "({} tracker{}, {} peer{})."
    if os.name != "nt":
        progress_template = "\r\x1b[K" + progress_template
    else:
        connections_template += "\n"
    try:
        while True:
            print(
                progress_template.format(total - root.missing_pieces),
                connections_template.format(
                    root.connected_trackers,
                    "s" if root.connected_trackers != 1 else "",
                    root.connected_peers,
                    "s" if root.connected_peers != 1 else "",
                ),
                end="",
                flush=True,
            )
            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        if not root.missing_pieces:
            # Print one last time, so that the terminal output reflects the
            # final state.
            print(progress_template.format(total), end=".\n", flush=True)
        raise


def build_file_tree(files):
    for file in files:
        file.path.parent.mkdir(parents=True, exist_ok=True)
        with file.path.open("a+b") as f:
            f.truncate(file.length)


async def download(metadata, peer_id, missing_pieces, max_peers, max_requests):
    """Download the torrent represented by `metadata`."""
    root = Root(
        metadata.info_hash,
        peer_id,
        metadata.announce_list,
        metadata.pieces,
        missing_pieces,
        max_peers,
        max_requests,
    )
    root.start()
    printer = asyncio.create_task(print_progress(root))
    try:
        loop = asyncio.get_running_loop()
        files = metadata.files
        # Delegate to a thread pool because asyncio has no direct support for
        # asynchronous file system operations.
        await loop.run_in_executor(None, functools.partial(build_file_tree, files))
        chunks = _metadata.make_chunks(metadata.pieces, files)
        async for piece, data in root.results:
            for chunk in chunks[piece]:
                await loop.run_in_executor(None, functools.partial(chunk.write, data))
    finally:
        printer.cancel()
        await asyncio.gather(printer, root.stop(), return_exceptions=True)
