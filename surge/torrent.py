import asyncio
import collections
import hashlib
import os
import random

import aiofiles

from . import actor
from . import metadata
from . import peer_protocol
from . import tracker_protocol


class Torrent(actor.Supervisor):
    """Coordinates the download.

    Parent: None
    Children:
        - an instance of `FileWriter`;
        - an instance of `PeerQueue`;
        - an instance of `PieceQueue`;
        - up to `max_peers` instances of `PeerConnection`.
    """

    def __init__(self, metainfo, torrent_state, available_pieces, *, max_peers=50):
        super().__init__()

        self._metainfo = metainfo
        self._torrent_state = torrent_state
        self._available_pieces = available_pieces

        metadata.ensure_files_exist(self._metainfo.folder, self._metainfo.files)

        self._file_writer = None
        self._peer_queue = None
        self._piece_queue = None

        self._done = asyncio.Event()
        self._peer_connection_slots = asyncio.Semaphore(max_peers)

        self._peer_to_connection = {}

    async def _spawn_peer_connections(self):
        while True:
            await self._peer_connection_slots.acquire()
            peer = await self._peer_queue.get()
            connection = PeerConnection(self, self._metainfo, self._torrent_state, peer)
            await self.spawn_child(connection)
            self._peer_to_connection[peer] = connection

    async def wait_done(self):
        """Wait until all pieces have been received."""
        await self._done.wait()

    ### Actor implementation

    async def _main_coro(self):
        self._file_writer = FileWriter(self, self._metainfo, self._available_pieces)
        self._peer_queue = PeerQueue(self, self._metainfo, self._torrent_state)
        self._piece_queue = PieceQueue(self, self._metainfo, self._available_pieces)
        for c in (self._file_writer, self._peer_queue, self._piece_queue):
            await self.spawn_child(c)
        await self._spawn_peer_connections()

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            for peer, connection in self._peer_to_connection.items():
                if connection is child:
                    break
            self._peer_to_connection.pop(peer)
            self._peer_connection_slots.release()
        else:
            self._crash(RuntimeError(f"Irreplaceable actor {repr(child)} crashed."))

    ### Messages from FileWriter

    def done(self):
        """Signal that every piece has been downloaded."""
        self._done.set()

    ### Messages from PieceQueue

    def cancel_piece(self, peers, piece):
        """Signal that `piece` does not need to be downloaded anymore;
        `peers` is a list of the peers that we are currently downloading
        `piece` from."""
        for peer in peers:
            self._peer_to_connection[peer].cancel_piece(piece)

    ### Messages from PeerConnection

    def set_have(self, peer, pieces):
        """Signal that the elements of `pieces` are available from `peer`."""
        self._piece_queue.set_have(peer, pieces)

    def add_to_have(self, peer, piece):
        """Signal that `piece` is available from `peer`."""
        self._piece_queue.add_to_have(peer, piece)

    def get_piece(self, peer):
        """Return a piece to download from `peer`."""
        return self._piece_queue.get_nowait(peer)

    def piece_done(self, peer, piece, data):
        """Signal that `data` was received and verified for `piece`."""
        self._file_writer.put_nowait(piece, data)
        self._piece_queue.task_done(peer, piece)


class FileWriter(actor.Actor):
    """Keeps track of received pieces and writes them to the file system.

    Parent: An instance of `Torrent`
    Children: None
    """

    def __init__(self, torrent, metainfo, available_pieces):
        super().__init__()

        self._torrent = torrent
        self._metainfo = metainfo

        self._outstanding = set(metainfo.pieces) - available_pieces
        self._piece_data = asyncio.Queue()

    async def _main_coro(self):
        piece_to_chunks = metadata.piece_to_chunks(
            self._metainfo.pieces, self._metainfo.files
        )
        while self._outstanding:
            piece, data = await self._piece_data.get()
            if piece not in self._outstanding:
                continue
            for c in piece_to_chunks[piece]:
                file_path = os.path.join(self._metainfo.folder, c.file.path)
                async with aiofiles.open(file_path, "rb+") as f:
                    await f.seek(c.file_offset)
                    await f.write(data[c.piece_offset : c.piece_offset + c.length])
            self._outstanding.remove(piece)
            n = len(self._metainfo.pieces)
            print(f"Progress: {n - len(self._outstanding)}/{n} pieces.")
        self._torrent.done()

    ### Queue interface

    def put_nowait(self, piece, data):
        """Signal that `data` was received and verified for `piece`."""
        self._piece_data.put_nowait((piece, data))


class PeerQueue(actor.Actor):
    """Requests peers from the tracker.

    Parent: An instance of `Torrent`
    Children: None
    """

    def __init__(self, torrent, metainfo, torrent_state):
        super().__init__()

        self._torrent = torrent
        self._metainfo = metainfo
        self._torrent_state = torrent_state

        self._peers = asyncio.Queue()

    ### Actor implementation

    async def _main_coro(self):
        seen_peers = set()
        while True:
            resp = await tracker_protocol.request_peers(
                self._metainfo, self._torrent_state
            )
            print(f"Got {len(resp.peers)} peer(s) from {resp.announce}.")
            for peer in set(resp.peers) - seen_peers:
                self._peers.put_nowait(peer)
                seen_peers.add(peer)
            await asyncio.sleep(resp.interval)

    ### Messages from Torrent

    async def get(self):
        """Return a peer that we have not yet connected to."""
        return await self._peers.get()


class PieceQueue(actor.Actor):
    """Holds the pieces that still need to be downloaded and their availability;
    exposes a queue interface for those pieces.

    Parent: An instance of `Torrent`
    Children: None
    """

    def __init__(self, torrent, metainfo, available_pieces):
        super().__init__()

        self._torrent = torrent

        # `self._available[peer]` is the set of pieces that `peer` has.
        self._available = collections.defaultdict(set)
        # `self._borrowers[piece]` is the set of peers that `piece` is being
        # downloaded from.
        self._borrowers = collections.defaultdict(set)
        self._outstanding = set(metainfo.pieces) - available_pieces

    def set_have(self, peer, pieces):
        """Signal that elements of `pieces` are available from `peer`."""
        self._available[peer] = set(pieces)

    def add_to_have(self, peer, piece):
        """Signal that `piece` is available from `peer`."""
        self._available[peer].add(piece)

    def drop_peer(self, peer):
        """Signal that `peer` was dropped."""
        self._available.pop(peer)
        for piece, borrowers in list(self._borrowers.items()):
            borrowers.discard(peer)
            if not borrowers:
                self._borrowers.pop(piece)
                self._outstanding.add(piece)

    ### Queue interface

    def get_nowait(self, peer):
        """Return a piece to download from `peer`.

        Returning `None` signals that the download is complete.
        """
        pool = self._outstanding or set(self._borrowers)
        if not pool:
            return None
        available = pool & self._available[peer]
        if available:
            piece = random.choice(list(available))
        else:
            piece = random.choice(list(pool))
        pool.remove(piece)
        self._borrowers[piece].add(peer)
        return piece

    def task_done(self, peer, piece):
        """Signal that `piece` was downloaded from `peer`."""
        if piece not in self._borrowers:
            return
        borrowers = self._borrowers.pop(piece)
        self._torrent.cancel_piece(borrowers - {peer}, piece)


class PeerConnection(actor.Actor):
    """Encapsulates the connection with a peer.

    Parent: An instance of `Torrent`
    Children:
        - an instance of `BlockQueue`
    """

    def __init__(self, torrent, metainfo, torrent_state, peer, *, max_requests=10):
        super().__init__()

        self._torrent = torrent
        self._metainfo = metainfo
        self._torrent_state = torrent_state

        self._peer = peer

        self._reader = None
        self._writer = None
        self._unchoked = asyncio.Event()

        self._block_queue = None

        self._block_to_timer = {}
        self._block_request_slots = asyncio.Semaphore(max_requests)

    async def _read_peer_message(self):
        len_prefix = int.from_bytes(await self._reader.readexactly(4), "big")
        message = await self._reader.readexactly(len_prefix)
        return peer_protocol.message_type(message), message[1:]

    async def _connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._peer.address, self._peer.port
        )

        self._writer.write(
            peer_protocol.handshake(
                self._torrent_state.info_hash, self._torrent_state.peer_id
            )
        )
        await self._writer.drain()

        response = await self._reader.readexactly(68)
        if not peer_protocol.valid_handshake(response, self._torrent_state.info_hash):
            raise ConnectionError("Peer sent invalid handshake.")

        # TODO: Accept peers that don't send a bitfield.
        message_type, payload = await self._read_peer_message()
        if message_type != "bitfield":
            raise ConnectionError("Peer didn't send a bitfield.")
        self._torrent.set_have(
            self._peer, peer_protocol.parse_bitfield(payload, self._metainfo.pieces),
        )

    async def _disconnect(self):
        if self._writer is None:
            return
        self._writer.close()
        try:
            await self._writer.wait_closed()
        # https://bugs.python.org/issue38856
        except (BrokenPipeError, ConnectionResetError):
            pass

    async def _unchoke(self):
        if self._unchoked.is_set():
            return
        self._writer.write(peer_protocol.interested())
        await self._writer.drain()
        await self._unchoked.wait()

    async def _request_timer(self, block, *, timeout=10):
        await asyncio.sleep(timeout)
        # Need to call `self._crash` here because this coroutine runs in a task
        # that is never awaited.
        self._crash(TimeoutError(f"Request for {block} from {self._peer} timed out."))

    async def _receive_blocks(self):
        while True:
            message_type, payload = await self._read_peer_message()
            if message_type == "choke":
                self._unchoked.clear()
                # TODO: Don't drop the peer if it chokes us.
                raise ConnectionError(f"Peer sent choke.")
            elif message_type == "unchoke":
                self._unchoked.set()
            elif message_type == "have":
                piece = peer_protocol.parse_have(payload, self._metainfo.pieces),
                self._torrent.add_to_have(self._peer, piece)
            elif message_type == "block":
                block, data = peer_protocol.parse_block(payload, self._metainfo.pieces)
                if block not in self._block_to_timer:
                    return
                self._block_to_timer.pop(block).cancel()
                self._block_request_slots.release()
                self._block_queue.task_done(block, data)

    async def _request_blocks(self):
        while True:
            await self._block_request_slots.acquire()
            block = self._block_queue.get_nowait()
            if block is None:
                break
            await self._unchoke()
            self._writer.write(peer_protocol.request(block))
            await self._writer.drain()
            self._block_to_timer[block] = asyncio.create_task(
                self._request_timer(block)
            )

    async def cancel_block(self, block):
        self._writer.write(peer_protocol.cancel(block))
        await self._writer.drain()

    ### Actor implementation

    async def _main_coro(self):
        await self._connect()
        self._block_queue = BlockQueue(self)
        await self.spawn_child(self._block_queue)
        await asyncio.gather(self._receive_blocks(), self._request_blocks())

    async def _on_stop(self):
        await self._disconnect()

    ### Messages from Torrent

    def cancel_piece(self, piece):
        """Signal that `piece` does not need to be downloaded anymore."""
        for block in list(self._block_to_timer):
            if block.piece == piece:
                self._block_to_timer.pop(block).cancel()
                # TODO: Send cancel message to the peer.
                self._block_request_slots.release()
        self._block_queue.cancel_piece(piece)

    ### Messages from BlockQueue

    def get_piece(self):
        """Return a piece to download."""
        return self._torrent.get_piece(self._peer)

    def piece_done(self, piece, data):
        """Signal that data was received (and verified) for piece."""
        self._torrent.piece_done(self._peer, piece, data)


class BlockQueue(actor.Actor):
    """Keeps track of blocks.

    Parent: An instance of `PeerConnection`
    Children: None
    """

    def __init__(self, peer_connection):
        super().__init__()

        self._peer_connection = peer_connection

        # Blocks that are to be downloaded.
        self._stack = []
        # `self._outstanding[piece]` is the set of blocks that are being
        # requested from the peer.
        self._outstanding = {}
        # `self._data[piece][block]` is the data that was received for `block`.
        self._data = {}

    def _on_piece_received(self, piece):
        self._outstanding.pop(piece)
        block_to_data = self._data.pop(piece)
        data = b"".join(block_to_data[block] for block in sorted(block_to_data))
        # Check that the data for `piece` is what it should be.
        if len(data) == piece.length and hashlib.sha1(data).digest() == piece.hash:
            self._peer_connection.piece_done(piece, data)
        else:
            raise ValueError(f"Peer sent invalid data.")

    def _restock(self, piece):
        blocks = metadata.blocks(piece)
        self._stack = blocks[::-1]
        self._outstanding[piece] = set(blocks)
        self._data[piece] = {}

    ### Queue interface

    def get_nowait(self):
        """Return a block that has not been downloaded yet.

        Returning `None` signals that the download is complete.
        """
        if not self._stack:
            piece = self._peer_connection.get_piece()
            if piece is None:
                return None
            self._restock(piece)
        return self._stack.pop()

    def task_done(self, block, data):
        """Signal that `data` was received for `block`."""
        piece = block.piece
        if piece in self._outstanding:
            self._data[piece][block] = data
            # Use discard, in case we get the same block multiple times.
            self._outstanding[piece].discard(block)
            if not self._outstanding[piece]:
                self._on_piece_received(piece)

    ### Messages from PeerConnection

    def cancel_piece(self, piece):
        """Signal that `piece` does not need to be downloaded anymore."""
        if piece in self._outstanding:
            self._stack = [block for block in self._stack if block.piece != piece]
            self._outstanding.pop(piece)
            self._data.pop(piece)
