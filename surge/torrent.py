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
    def __init__(self, metainfo, *, max_peers=50):
        super().__init__()

        self._metainfo = metainfo
        if self._metainfo.peer_id is None:
            self._metainfo.peer_id = secrets.token_bytes(20)

        metadata.ensure_files_exist(self._metainfo.folder, self._metainfo.files)

        self._file_writer = None
        self._piece_queue = None
        self._peer_queue = None

        self._done = asyncio.Event()
        self._peer_connection_slots = asyncio.Semaphore(max_peers)

    async def _spawn_peer_connections(self):
        while True:
            await self._peer_connection_slots.acquire()
            peer = await self._peer_queue.get()
            await self.spawn_child(PeerConnection(self, self._metainfo, peer))

    async def wait_done(self):
        await self._done.wait()

    ### Actor implementation

    async def _main_coro(self):
        self._file_writer = FileWriter(self, self._metainfo)
        self._piece_queue = PieceQueue(self, self._metainfo)
        self._peer_queue = PeerQueue(self, self._metainfo)
        for c in (self._file_writer, self._piece_queue, self._peer_queue):
            await self.spawn_child(c)
        await self._spawn_peer_connections()

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            self._peer_connection_slots.release()

    ### Messages from FileWriter

    def done(self):
        self._done.set()

    ### Messages from PieceQueue

    def cancel_piece(self, peers, piece):
        for child in self.children:
            if isinstance(child, PeerConnection) and child.peer in peers:
                child.cancel_piece(piece)

    ### Messages from PeerConnection

    def set_have(self, peer, pieces):
        self._piece_queue.set_have(peer, pieces)

    def get_piece(self, peer):
        return self._piece_queue.get_nowait(peer)

    def piece_done(self, peer, piece, data):
        self._file_writer.piece_data(piece, data)
        self._piece_queue.task_done(peer, piece)

    def add_to_have(self, peer, piece):
        self._piece_queue.add_to_have(peer, piece)


class FileWriter(actor.Actor):
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

    def piece_data(self, piece, data):
        self._piece_data.put_nowait((piece, data))


class PeerQueue(actor.Actor):
    def __init__(self, torrent, metainfo):
        super().__init__()

        self._torrent = torrent
        self._metainfo = metainfo

        self._peers = asyncio.Queue()

    ### Actor implementation

    async def _main_coro(self):
        seen_peers = set()
        while True:
            peers, interval = await tracker_protocol.request_peers(self._metainfo)
            print(f"Got {len(peers)} peers from {self._metainfo.announce}.")
            for peer in set(peers) - seen_peers:
                self._peers.put_nowait(peer)
                seen_peers.add(peer)
            await asyncio.sleep(interval)

    ### Queue interface

    async def get(self):
        return await self._peers.get()


class PieceQueue(actor.Actor):
    def __init__(self, torrent, metainfo):
        super().__init__()

        self._torrent = torrent

        self._available = collections.defaultdict(set)
        self._borrowers = collections.defaultdict(set)
        if metainfo.missing_pieces is not None:
            self._outstanding = set(metainfo.missing_pieces)
        else:
            self._outstanding = set(metainfo.pieces)

    def set_have(self, peer, pieces):
        self._available[peer] = set(pieces)

    def add_to_have(self, peer, piece):
        self._available[peer].add(piece)

    def drop_peer(self, peer):
        self._available.pop(peer)
        for piece, borrowers in list(self._borrowers.items()):
            borrowers.discard(piece)
            if not borrowers:
                self._borrowers.pop(piece)
                self._outstanding.add(piece)

    ### Queue interface

    def get_nowait(self, peer):
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
        if piece not in self._borrowers:
            return
        borrowers = self._borrowers.pop(piece)
        self._torrent.cancel_piece(borrowers - {peer}, piece)


async def read_peer_message(reader):
    len_prefix = int.from_bytes(await reader.readexactly(4), "big")
    message = await reader.readexactly(len_prefix)
    return peer_protocol.message_type(message), message[1:]


class PeerConnection(actor.Actor):
    def __init__(self, torrent, metainfo, peer, *, max_requests=10):
        super().__init__()

        self._torrent = torrent
        self._metainfo = metainfo

        self.peer = peer

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
            self.peer.address, self.peer.port
        )

        self._writer.write(
            peer_protocol.handshake(self._metainfo.info_hash, self._metainfo.peer_id)
        )
        await self._writer.drain()

        response = await self._reader.readexactly(68)
        if not peer_protocol.valid_handshake(response, self._metainfo.info_hash):
            self._crash(ConnectionError("Peer sent invalid handshake."))
            return

        # TODO: Accept peers that don't send a bitfield.
        message_type, payload = await read_peer_message(self._reader)
        if message_type != "bitfield":
            self._crash(ConnectionError("Peer didn't send a bitfield."))
            return
        self._torrent.set_have(
            self.peer, peer_protocol.parse_bitfield(payload, self._metainfo.pieces),
        )

    async def _disconnect(self):
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            # https://bugs.python.org/issue38856
            except ConnectionResetError:
                pass

    async def _request_timer(self, block, *, timeout=10):
        await asyncio.sleep(timeout)
        self._crash(TimeoutError(f"Request for {block} from {self.peer} timed out."))

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
        for block in list(self._block_to_timer):
            if block.piece == piece:
                self._block_to_timer.pop(block).cancel()
                # TODO: Send cancel message to the peer.
                self._block_request_slots.release()
        self._block_queue.cancel_piece(piece)

    ### Messages from BlockQueue

    def get_piece(self):
        return self._torrent.get_piece(self.peer)

    def piece_done(self, piece, data):
        self._torrent.piece_done(self.peer, piece, data)

    ### Messages from BlockReceiver

    def received_choke(self):
        self._unchoked.clear()

    def received_unchoke(self):
        self._unchoked.set()

    def received_block(self, block, data):
        if block not in self._block_to_timer:
            return
        self._block_to_timer.pop(block).cancel()
        self._block_request_slots.release()
        self._block_queue.task_done(block, data)

    def add_to_have(self, piece):
        self._torrent.add_to_have(self.peer, piece)

    ### Messages from BlockRequester

    async def get_block(self):
        await self._block_request_slots.acquire()
        return self._block_queue.get_nowait()

    async def unchoke(self):
        if self._unchoked.is_set():
            return
        self._writer.write(peer_protocol.interested())
        await self._writer.drain()
        await self._unchoked.wait()

    def requested_block(self, block):
        self._block_to_timer[block] = asyncio.create_task(self._request_timer(block))


class BlockQueue(actor.Actor):
    def __init__(self, peer_connection):
        super().__init__()

        self._peer_connection = peer_connection

        self._stack = []
        self._outstanding = {}  # Piece -> Set[Block]
        self._data = {}  # Piece -> (Block -> bytes)

    def _on_piece_received(self, piece):
        self._outstanding.pop(piece)
        block_to_data = self._data.pop(piece)
        data = b"".join(block_to_data[block] for block in sorted(block_to_data))
        if len(data) == piece.length and hashlib.sha1(data).digest() == piece.hash:
            self._peer_connection.piece_done(piece, data)
        else:
            self._crash(ValueError(f"Peer sent invalid data."))

    def _restock(self, piece):
        blocks = metadata.blocks(piece)
        self._stack = blocks[::-1]
        self._outstanding[piece] = set(blocks)
        self._data[piece] = {}

    ### Queue interface

    def get_nowait(self):
        if not self._stack:
            piece = self._peer_connection.get_piece()
            if piece is None:
                return None
            self._restock(piece)
        return self._stack.pop()

    def cancel_task(self, block):
        if block.piece in self._outstanding:
            self._stack.append(block)

    def task_done(self, block, data):
        piece = block.piece
        if piece in self._outstanding:
            self._data[piece][block] = data
            # Use discard, in case we get the same block multiple times.
            self._outstanding[piece].discard(block)
            if not self._outstanding[piece]:
                self._on_piece_received(piece)

    ### Messages from PeerConnection

    def cancel_piece(self, piece):
        if piece in self._outstanding:
            self._stack = [block for block in self._stack if block.piece != piece]
            self._outstanding.pop(piece)
            self._data.pop(piece)


class BlockReceiver(actor.Actor):
    def __init__(self, peer_connection, metainfo, reader):
        super().__init__()

        self._peer_connection = peer_connection
        self._metainfo = metainfo
        self._reader = reader

    async def _listen_to_peer(self):
        while True:
            message_type, payload = await read_peer_message(self._reader)
            if message_type == "choke":
                self._peer_connection.received_choke()
                # TODO: Don't drop the peer if it chokes us.
                self._crash(ConnectionError(f"Peer sent choke."))
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

    ### Actor implementation

    async def _main_coro(self):
        try:
            await self._listen_to_peer()
        except Exception as e:
            self._crash(e)


class BlockRequester(actor.Actor):
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
