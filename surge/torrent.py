import asyncio
import collections
import hashlib
import os
import random
import secrets

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

    def __init__(self, metainfo, *, max_peers=50):
        super().__init__()

        self._metainfo = metainfo
        if self._metainfo.peer_id is None:
            self._metainfo.peer_id = secrets.token_bytes(20)

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
            connection = PeerConnection(self, self._metainfo, peer)
            await self.spawn_child(connection)
            self._peer_to_connection[peer] = connection

    async def wait_done(self):
        """Wait until all pieces have been received."""
        await self._done.wait()

    ### Actor implementation

    async def _main_coro(self):
        self._file_writer = FileWriter(self, self._metainfo)
        self._peer_queue = PeerQueue(self, self._metainfo)
        self._piece_queue = PieceQueue(self, self._metainfo)
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

    def __init__(self, torrent, metainfo):
        super().__init__()

        self._torrent = torrent
        self._metainfo = metainfo

        self._piece_data = asyncio.Queue()

    async def _main_coro(self):
        piece_to_chunks = metadata.piece_to_chunks(
            self._metainfo.pieces, self._metainfo.files
        )
        if self._metainfo.missing_pieces is not None:
            outstanding_pieces = set(self._metainfo.missing_pieces)
        else:
            outstanding_pieces = set(self._metainfo.pieces)
        while outstanding_pieces:
            piece, data = await self._piece_data.get()
            if piece not in outstanding_pieces:
                continue
            for c in piece_to_chunks[piece]:
                file_path = os.path.join(self._metainfo.folder, c.file.path)
                async with aiofiles.open(file_path, "rb+") as f:
                    await f.seek(c.file_offset)
                    await f.write(data[c.piece_offset : c.piece_offset + c.length])
            outstanding_pieces.remove(piece)
            self._metainfo.downloaded += piece.length
            n = len(self._metainfo.pieces)
            print(f"Progress: {n - len(outstanding_pieces)}/{n} pieces.")
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

    def __init__(self, torrent, metainfo):
        super().__init__()

        self._torrent = torrent
        self._metainfo = metainfo

        self._peers = asyncio.Queue()

    ### Actor implementation

    async def _main_coro(self):
        seen_peers = set()
        while True:
            resp = await tracker_protocol.request_peers(self._metainfo)
            if resp.error is not None:
                raise ConnectionError("Tracker response: {resp.error}")
            print(f"Got {len(resp.peers)} peer(s) from {self._metainfo.announce}.")
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

    def __init__(self, torrent, metainfo):
        super().__init__()

        self._torrent = torrent

        # `self._available[peer]` is the set of pieces that `peer` has.
        self._available = collections.defaultdict(set)
        # `self._borrowers[piece]` is the set of peers that `piece` is being
        # downloaded from.
        self._borrowers = collections.defaultdict(set)
        # TODO: Change the metainfo class so that this branch is not needed.
        if metainfo.missing_pieces is not None:
            self._outstanding = set(metainfo.missing_pieces)
        else:
            self._outstanding = set(metainfo.pieces)

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


async def read_peer_message(reader):
    # Used by `PeerConnection` and `BlockReceiver`.
    len_prefix = int.from_bytes(await reader.readexactly(4), "big")
    message = await reader.readexactly(len_prefix)
    return peer_protocol.message_type(message), message[1:]


class PeerConnection(actor.Actor):
    """Encapsulates the connection with a peer.

    Parent: An instance of `Torrent`
    Children:
        - an instance of `BlockQueue`
        - an instance of `BlockRequester`
        - an instance of `BlockReceiver`
    """

    def __init__(self, torrent, metainfo, peer, *, max_requests=10):
        super().__init__()

        self._torrent = torrent
        self._metainfo = metainfo

        self._peer = peer

        self._reader = None
        self._writer = None
        self._unchoked = asyncio.Event()

        self._block_queue = None
        self._block_receiver = None
        self._block_requester = None

        self._block_to_timer = {}
        self._block_request_slots = asyncio.Semaphore(max_requests)

    async def _connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._peer.address, self._peer.port
        )

        self._writer.write(
            peer_protocol.handshake(self._metainfo.info_hash, self._metainfo.peer_id)
        )
        await self._writer.drain()

        response = await self._reader.readexactly(68)
        if not peer_protocol.valid_handshake(response, self._metainfo.info_hash):
            raise ConnectionError("Peer sent invalid handshake.")

        # TODO: Accept peers that don't send a bitfield.
        message_type, payload = await read_peer_message(self._reader)
        if message_type != "bitfield":
            raise ConnectionError("Peer didn't send a bitfield.")
        self._torrent.set_have(
            self._peer, peer_protocol.parse_bitfield(payload, self._metainfo.pieces),
        )

    async def _disconnect(self):
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            # https://bugs.python.org/issue38856
            except (BrokenPipeError, ConnectionResetError):
                pass

    async def _request_timer(self, block, *, timeout=10):
        await asyncio.sleep(timeout)
        # Need to call `self._crash` here because this coroutine runs in a task
        # that is never awaited.
        self._crash(TimeoutError(f"Request for {block} from {self._peer} timed out."))

    ### Actor implementation

    async def _main_coro(self):
        await self._connect()
        self._block_queue = BlockQueue(self)
        self._block_receiver = BlockReceiver(self, self._metainfo, self._reader)
        self._block_requester = BlockRequester(self, self._writer)
        for c in (self._block_queue, self._block_receiver, self._block_requester):
            await self.spawn_child(c)

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

    ### Messages from BlockReceiver

    def received_choke(self):
        """Signal that the peer started choking us."""
        self._unchoked.clear()

    def received_unchoke(self):
        """Signal that the peer stopped choking us."""
        self._unchoked.set()

    def received_block(self, block, data):
        """Signal that `data` was received for `block`."""
        if block not in self._block_to_timer:
            return
        self._block_to_timer.pop(block).cancel()
        self._block_request_slots.release()
        self._block_queue.task_done(block, data)

    def add_to_have(self, piece):
        """Signal that `piece` is available from the peer."""
        self._torrent.add_to_have(self._peer, piece)

    ### Messages from BlockRequester

    async def get_block(self):
        """Wait until a block request slot opens up and then return a block that
        is to be requested from the peer."""
        await self._block_request_slots.acquire()
        return self._block_queue.get_nowait()

    async def unchoke(self):
        """If the peer is choking us, send an unchoke request and wait for the
        peer to stop doing so."""
        if self._unchoked.is_set():
            return
        self._writer.write(peer_protocol.interested())
        await self._writer.drain()
        await self._unchoked.wait()

    def requested_block(self, block):
        """Signal that `block` was requested from the peer."""
        self._block_to_timer[block] = asyncio.create_task(self._request_timer(block))


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


class BlockReceiver(actor.Actor):
    """Receives messages from the peer and relays them to its parent.

    Parent: An instance of `PeerConnection`
    Children: None
    """

    def __init__(self, peer_connection, metainfo, reader):
        super().__init__()

        self._peer_connection = peer_connection
        self._metainfo = metainfo
        self._reader = reader

    ### Actor implementation

    async def _main_coro(self):
        while True:
            message_type, payload = await read_peer_message(self._reader)
            if message_type == "choke":
                self._peer_connection.received_choke()
                # TODO: Don't drop the peer if it chokes us.
                raise ConnectionError(f"Peer sent choke.")
            elif message_type == "unchoke":
                self._peer_connection.received_unchoke()
            elif message_type == "have":
                self._peer_connection.add_to_have(
                    peer_protocol.parse_have(payload, self._metainfo.pieces),
                )
            elif message_type == "block":
                self._peer_connection.received_block(
                    *peer_protocol.parse_block(payload, self._metainfo.pieces)
                )


class BlockRequester(actor.Actor):
    """Sends block requests to the peer.

    Parent: An instance of `PeerConnection`
    Children: None
    """

    def __init__(self, peer_connection, writer):
        super().__init__()

        self._peer_connection = peer_connection
        self._writer = writer

    ### Actor implementation

    async def _main_coro(self):
        while True:
            block = await self._peer_connection.get_block()
            if block is None:
                break
            await self._peer_connection.unchoke()
            self._writer.write(peer_protocol.request(block))
            await self._writer.drain()
            self._peer_connection.requested_block(block)

    ### Messages from PeerConnection

    async def cancel_block(self, block):
        self._writer.write(peer_protocol.cancel(block))
        await self._writer.drain()
