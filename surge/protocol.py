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
                progress_template.format(total - len(root.missing_pieces)),
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
        file.path.parent.mkdir(exist_ok=True)
        with file.path.open("a+b") as f:
            f.truncate(file.length)


async def download(metadata, peer_id, missing_pieces, max_peers, max_requests):
    """Spin up a `Root` and write downloaded pieces to the file system."""
    root = Root(metadata, peer_id, missing_pieces, max_peers, max_requests)
    root.start()
    printer = asyncio.create_task(print_progress(root))
    try:
        if missing_pieces:
            loop = asyncio.get_running_loop()
            # Delegate to a thread pool because asyncio has no direct support for
            # asynchronous file system operations.
            await loop.run_in_executor(
                None, functools.partial(build_file_tree, metadata.files)
            )
            chunks = _metadata.make_chunks(metadata.pieces, metadata.files)
            async for piece, data in root.results:
                for chunk in chunks[piece]:
                    await loop.run_in_executor(
                        None, functools.partial(_metadata.write_chunk, chunk, data)
                    )
    finally:
        await root.stop()
        printer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await printer


class Root:
    def __init__(self, metadata, peer_id, missing_pieces, max_peers, max_requests):
        self.pieces = metadata.pieces
        self.info_hash = metadata.info_hash
        self.peer_id = peer_id
        self.missing_pieces = missing_pieces
        self.max_peers = max_peers
        self.max_requests = max_requests

        # A `Channel` that holds up to `max_peers` downloaded pieces. If the
        # channel fills up, `Node`s will hold off on downloading more pieces
        # until the file system has caught up.
        self.results = Channel(max_peers)

        self._announce_list = metadata.announce_list
        self._trackers = set()
        self._seen_peers = set()
        self._new_peers = asyncio.Queue(max_peers)

        self._nodes = set()
        # In endgame mode, multiple `Node`s can be downloading the same piece;
        # as soon as one finishes downloading that piece, the other `Node`s
        # need to be notified. Therefore we keep track of which `Node`s are
        # downloading any given piece.
        self._node_to_pieces = {}
        self._piece_to_nodes = collections.defaultdict(set)

    @property
    def connected_trackers(self):
        """The number of connected trackers."""
        return len(self._trackers)

    @property
    def connected_peers(self):
        """The number of connected peers."""
        return len(self._nodes)

    async def put_peer(self, peer):
        if peer in self._seen_peers:
            return
        self._seen_peers.add(peer)
        await self._new_peers.put(peer)
        self.maybe_add_node()

    def get_piece(self, node, available):
        """Return a fresh piece to download.

        Raise `IndexError` if there are no more pieces to download.
        """
        in_progress = set(self._piece_to_nodes)
        # Strict endgame mode: only send duplicate requests if every missing
        # piece is already being requested from some peer.
        piece = random.choice(
            tuple(
                (
                    (self.missing_pieces - in_progress or in_progress)
                    - self._node_to_pieces[node]
                )
                & available
            )
        )
        self._piece_to_nodes[piece].add(node)
        self._node_to_pieces[node].add(piece)
        return piece

    async def put_piece(self, node, piece, data):
        """Deliver a downloaded piece.

        If any other nodes are in the process of downloading `piece`, those
        downloads are canceled.
        """
        if piece not in self.missing_pieces:
            return
        self.missing_pieces.remove(piece)
        self._node_to_pieces[node].remove(piece)
        for other in self._piece_to_nodes.pop(piece) - {node}:
            self._node_to_pieces[other].remove(piece)
            other.remove_piece(piece)
        await self.results.put((piece, data))
        if not self.missing_pieces:
            await self.results.close()

    def remove_tracker(self, task):
        self._trackers.remove(task)

    def maybe_add_node(self):
        if len(self._nodes) < self.max_peers and self._new_peers.qsize():
            node = Node(self, self._new_peers.get_nowait())
            node.start()
            self._nodes.add(node)
            self._node_to_pieces[node] = set()

    def remove_node(self, node):
        self._nodes.remove(node)
        for piece in self._node_to_pieces.pop(node):
            self._piece_to_nodes[piece].remove(node)
            if not self._piece_to_nodes[piece]:
                self._piece_to_nodes.pop(piece)
        self.maybe_add_node()

    def start(self):
        parameters = tracker.Parameters(self.info_hash, self.peer_id)
        for announce in self._announce_list:
            url = urllib.parse.urlparse(announce)
            if url.scheme in ("http", "https"):
                coroutine = tracker.request_peers_http(self, url, parameters)
            elif url.scheme == "udp":
                coroutine = tracker.request_peers_udp(self, url, parameters)
            else:
                raise ValueError("Unsupported announce.")
            self._trackers.add(asyncio.create_task(coroutine))

    async def stop(self):
        for task in tuple(self._trackers):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for node in tuple(self._nodes):
            await node.stop()


class Progress:
    """Helper class that keeps track of a single piece's progress."""

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
    CHOKED = enum.auto()
    INTERESTED = enum.auto()
    UNCHOKED = enum.auto()
    NO_REQUESTS_POSSIBLE = enum.auto()


class Node:
    def __init__(self, root, peer):
        self.root = root
        self.peer = peer

        self._progress = {}
        self._requested = set()
        self._stack = []

        self._task = None

    def add_piece(self, piece):
        """Add `piece` to the download queue."""
        blocks = tuple(_metadata.blocks(piece))
        self._progress[piece] = Progress(piece, blocks)
        self._stack.extend(reversed(blocks))

    def remove_piece(self, piece):
        """Remove `piece` from the download queue."""
        self._progress.pop(piece)

        def predicate(block):
            return block.piece != piece

        self._requested = set(filter(predicate, self._requested))
        self._stack = list(filter(predicate, self._stack))

    def reset_progress(self):
        in_progress = tuple(self._progress)
        self._progress.clear()
        self._requested.clear()
        self._stack.clear()
        for piece in in_progress:
            self.add_piece(piece)

    @property
    def can_request(self):
        return len(self._requested) < self.root.max_requests

    def get_block(self):
        block = self._stack.pop()
        self._requested.add(block)
        return block

    def put_block(self, block, data):
        if block not in self._requested:
            return None
        self._requested.remove(block)
        piece = block.piece
        progress = self._progress[piece]
        progress.add(block, data)
        if not progress.done:
            return None
        data = self._progress.pop(piece).data
        if _metadata.valid(piece, data):
            return (piece, data)
        raise ValueError("Invalid data.")

    async def _main(self):
        try:
            async with open_stream(self.peer) as stream:
                pieces = self.root.pieces
                info_hash = self.root.info_hash
                peer_id = self.root.peer_id
                await stream.write(messages.Handshake(0, info_hash, peer_id))
                received = await asyncio.wait_for(stream.read_handshake(), 30)
                if received.info_hash != info_hash:
                    raise ValueError("Wrong 'info_hash'.")
                available = set()
                # Wait for the peer to tell us which pieces it has. This is not
                # mandated by the specification, but makes requesting pieces
                # much easier.
                while True:
                    received = await asyncio.wait_for(stream.read(), 30)
                    if isinstance(received, messages.Have):
                        available.add(pieces[received.index])
                        break
                    if isinstance(received, messages.Bitfield):
                        for i in received.to_indices():
                            available.add(pieces[i])
                        break
                state = State.CHOKED
                while True:
                    if state is State.CHOKED:
                        await stream.write(messages.Interested())
                        state = State.INTERESTED
                    elif state is State.UNCHOKED and self.can_request:
                        try:
                            block = self.get_block()
                        except IndexError:
                            try:
                                piece = self.root.get_piece(self, available)
                            except IndexError:
                                state = State.NO_REQUESTS_POSSIBLE
                            else:
                                self.add_piece(piece)
                        else:
                            await stream.write(messages.Request.from_block(block))
                    else:
                        received = await asyncio.wait_for(stream.read(), 30)
                        if isinstance(received, messages.Choke):
                            self.reset_progress()
                            state = State.CHOKED
                        elif isinstance(received, messages.Unchoke):
                            if state is State.INTERESTED:
                                state = State.UNCHOKED
                        elif isinstance(received, messages.Have):
                            available.add(pieces[received.index])
                            if state is State.NO_REQUESTS_POSSIBLE:
                                state = State.UNCHOKED
                        elif isinstance(received, messages.Block):
                            result = self.put_block(
                                _metadata.Block(
                                    pieces[received.index],
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
