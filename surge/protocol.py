import asyncio
import collections
import contextlib
import enum
import functools
import random
import urllib.parse

from . import _metadata
from . import messages
from . import tracker
from .channel import Channel
from .stream import open_stream


async def print_progress(root):
    """Periodically poll `root` and print the download progress to stdout."""
    total = len(root.pieces)
    progress_template = "\r\x1b[KDownload progress: {{}}/{} pieces".format(total)
    connections_template = "({} tracker{}, {} peer{})"
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
                end=".",
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
        if missing_pieces:
            loop = asyncio.get_running_loop()
            # Delegate to a thread pool because asyncio has no direct support
            # for asynchronous file system operations.
            await loop.run_in_executor(
                None, functools.partial(build_file_tree, metadata.files)
            )
            chunks = _metadata.make_chunks(metadata.pieces, metadata.files)
            async for piece, data in root.results:
                for chunk in chunks[piece]:
                    await loop.run_in_executor(
                        None, functools.partial(chunk.write, data)
                    )
    finally:
        printer.cancel()
        await asyncio.gather(printer, root.stop(), return_exceptions=True)


class Root:
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
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.pieces = pieces
        self.max_peers = max_peers
        self.max_requests = max_requests

        # A `Channel` that holds up to `max_peers` downloaded pieces. If the
        # channel fills up, `Node`s will hold off on downloading more pieces
        # until the file system has caught up.
        self.results = Channel(max_peers)

        self._announce_list = announce_list
        self._parameters = tracker.Parameters(info_hash, peer_id)
        self._trackers = set()
        self._seen_peers = set()
        self._new_peers = asyncio.Queue(max_peers)

        self._stopped = False
        self._nodes = set()
        self._missing_pieces = set(missing_pieces)
        # In endgame mode, multiple `Node`s can be downloading the same piece;
        # as soon as one finishes downloading that piece, the other `Node`s
        # need to be notified. Therefore we keep track of which `Node`s are
        # downloading any given piece.
        self._node_to_pieces = {}
        self._piece_to_nodes = collections.defaultdict(set)

    @property
    def missing_pieces(self):
        """The number of missing pieces."""
        return len(self._missing_pieces)

    @property
    def connected_trackers(self):
        """The number of connected trackers."""
        return len(self._trackers)

    @property
    def connected_peers(self):
        """The number of connected peers."""
        return len(self._nodes)

    def _maybe_add_node(self):
        if self._stopped:
            return
        if len(self._nodes) < self.max_peers and self._new_peers.qsize():
            node = Node(
                self,
                self._new_peers.get_nowait(),
                self.info_hash,
                self.peer_id,
                self.pieces,
                self.max_requests,
            )
            node.start()
            self._nodes.add(node)
            self._node_to_pieces[node] = set()

    async def put_peer(self, peer):
        if peer in self._seen_peers:
            return
        self._seen_peers.add(peer)
        await self._new_peers.put(peer)
        self._maybe_add_node()

    def remove_tracker(self, task):
        self._trackers.remove(task)

    def get_piece(self, node, available):
        """Return a piece to download next.

        Raise `IndexError` if there are no additional pieces to download.
        """
        pool = self._missing_pieces & (available - self._node_to_pieces[node])
        piece = random.choice(tuple(pool - set(self._piece_to_nodes) or pool))
        self._piece_to_nodes[piece].add(node)
        self._node_to_pieces[node].add(piece)
        return piece

    async def put_piece(self, node, piece, data):
        """Deliver a downloaded piece.

        If any other nodes are in the process of downloading `piece`, those
        downloads are canceled.
        """
        if piece not in self._missing_pieces:
            return
        self._missing_pieces.remove(piece)
        self._node_to_pieces[node].remove(piece)
        for other in self._piece_to_nodes.pop(piece) - {node}:
            self._node_to_pieces[other].remove(piece)
            other.remove_piece(piece)
        await self.results.put((piece, data))
        if not self._missing_pieces:
            await self.results.close()

    def remove_node(self, node):
        self._nodes.remove(node)
        for piece in self._node_to_pieces.pop(node):
            self._piece_to_nodes[piece].remove(node)
            if not self._piece_to_nodes[piece]:
                self._piece_to_nodes.pop(piece)
        self._maybe_add_node()

    def start(self):
        for url in map(urllib.parse.urlparse, self._announce_list):
            # Note that `urllib.parse.urlparse` lower-cases the scheme, so
            # exact comparison is correct here (and elsewhere).
            if url.scheme in ("http", "https"):
                coroutine = tracker.request_peers_http(self, url, self._parameters)
            elif url.scheme == "udp":
                coroutine = tracker.request_peers_udp(self, url, self._parameters)
            else:
                raise ValueError("Wrong scheme.")
            self._trackers.add(asyncio.create_task(coroutine))

    async def stop(self):
        self._stopped = True
        for task in self._trackers:
            task.cancel()
        asyncio.gather(
            *self._trackers,
            *(node.stop() for node in self._nodes),
            return_exceptions=True,
        )


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


class State(enum.IntEnum):
    """Connection state after handshakes are exchanged."""

    CHOKED = enum.auto()
    INTERESTED = enum.auto()
    UNCHOKED = enum.auto()
    # There are no more blocks to request.
    PASSIVE = enum.auto()


class Node:
    def __init__(self, root, peer, info_hash, peer_id, pieces, max_requests):
        self.root = root

        self.peer = peer
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.pieces = pieces
        self.max_requests = max_requests

        self._progress = {}
        self._requested = set()
        self._queue = collections.deque()

        # Task running the `_main` coroutine.
        self._task = None

    def add_piece(self, piece):
        """Add `piece` to the download queue."""
        blocks = set(_metadata.yield_blocks(piece))
        self._progress[piece] = Progress(piece, blocks)
        self._queue.extendleft(blocks)

    def remove_piece(self, piece):
        """Remove `piece` from the download queue."""
        self._progress.pop(piece)

        def predicate(block):
            return block.piece != piece

        self._requested = set(filter(predicate, self._requested))
        self._queue = collections.deque(filter(predicate, self._queue))

    def _reset_progress(self):
        in_progress = tuple(self._progress)
        self._progress.clear()
        self._requested.clear()
        self._queue.clear()
        for piece in in_progress:
            self.add_piece(piece)

    @property
    def _can_request(self):
        return len(self._requested) < self.max_requests

    def _get_block(self):
        """Return a block to download next.

        Raise `IndexError` if the block queue is empty.
        """
        block = self._queue.pop()
        self._requested.add(block)
        return block

    def _put_block(self, block, data):
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

    async def _main(self):
        try:
            async with open_stream(self.peer) as stream:
                await stream.write(messages.Handshake(0, self.info_hash, self.peer_id))
                received = await asyncio.wait_for(stream.read_handshake(), 30)
                if received.info_hash != self.info_hash:
                    raise ValueError("Wrong 'info_hash'.")
                available = set()
                # Wait for the peer to tell us which pieces it has. This is not
                # mandated by the specification, but makes requesting pieces
                # much easier.
                while True:
                    received = await asyncio.wait_for(stream.read(), 30)
                    if isinstance(received, messages.Have):
                        available.add(self.pieces[received.index])
                        break
                    if isinstance(received, messages.Bitfield):
                        for i in received.to_indices():
                            available.add(self.pieces[i])
                        break
                state = State.CHOKED
                while True:
                    if state is State.CHOKED:
                        await stream.write(messages.Interested())
                        state = State.INTERESTED
                    elif state is State.UNCHOKED and self._can_request:
                        try:
                            block = self._get_block()
                        except IndexError:
                            try:
                                piece = self.root.get_piece(self, available)
                            except IndexError:
                                state = State.PASSIVE
                            else:
                                self.add_piece(piece)
                        else:
                            await stream.write(messages.Request.from_block(block))
                    else:
                        received = await asyncio.wait_for(stream.read(), 30)
                        if isinstance(received, messages.Choke):
                            self._reset_progress()
                            state = State.CHOKED
                        elif isinstance(received, messages.Unchoke):
                            if state is not State.PASSIVE:
                                state = State.UNCHOKED
                        elif isinstance(received, messages.Have):
                            available.add(self.pieces[received.index])
                            if state is State.PASSIVE:
                                state = State.UNCHOKED
                        elif isinstance(received, messages.Block):
                            result = self._put_block(
                                _metadata.Block(
                                    self.pieces[received.index],
                                    received.begin,
                                    len(received.data),
                                ),
                                received.data,
                            )
                            if result is not None:
                                await self.root.put_piece(self, *result)
        except Exception:
            pass
        finally:
            self.root.remove_node(self)

    def start(self):
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._main())

    async def stop(self):
        if (task := self._task) is None:
            return
        self._task = None
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
