from typing import Iterable, Optional, Set

import asyncio
import collections
import hashlib
import os
import random

import aiofiles

from . import actor
from . import metadata
from . import peer_protocol
from . import peer_queue
from . import tracker_protocol


class Download(actor.Supervisor):
    def __init__(
        self,
        metainfo: metadata.Metainfo,
        tracker_params: metadata.TrackerParameters,
        available_pieces: Set[metadata.Piece],
        *,
        max_peers: int = 50,
    ):
        super().__init__()

        self._metainfo = metainfo
        self._tracker_params = tracker_params
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
            connection = PeerConnection(
                self, self._metainfo, self._tracker_params, peer
            )
            await self.spawn_child(connection)
            self._peer_to_connection[peer] = connection

    async def wait_done(self):
        """Wait until all pieces have been received."""
        await self._done.wait()

    ### Actor implementation

    async def _main_coro(self):
        self._file_writer = FileWriter(self, self._metainfo, self._available_pieces)
        self._peer_queue = peer_queue.PeerQueue(
            self._metainfo.announce_list, self._tracker_params
        )
        self._piece_queue = PieceQueue(self, self._metainfo, self._available_pieces)
        for c in (self._file_writer, self._peer_queue, self._piece_queue):
            await self.spawn_child(c)
        await self._spawn_peer_connections()

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            for peer, connection in self._peer_to_connection.items():
                if connection is child:
                    break
            self._piece_queue.drop_peer(peer)
            self._peer_to_connection.pop(peer)
            self._peer_connection_slots.release()
        else:
            self._crash(RuntimeError(f"Irreplaceable actor {repr(child)} crashed."))

    ### Messages from FileWriter

    def done(self):
        """Signal that every piece has been downloaded."""
        self._done.set()

    ### Messages from PieceQueue

    def cancel_piece(self, peers: Iterable[metadata.Peer], piece: metadata.Piece):
        """Signal that `piece` does not need to be downloaded anymore; `peers`
        is a iterable for the peers that we are currently downloading `piece`
        from."""
        for peer in peers:
            self._peer_to_connection[peer].cancel_piece(piece)

    ### Messages from PeerConnection

    def set_have(self, peer: metadata.Peer, pieces: Iterable[metadata.Piece]):
        """Signal that the elements of `pieces` are available from `peer`."""
        self._piece_queue.set_have(peer, pieces)

    def add_to_have(self, peer: metadata.Peer, piece: metadata.Piece):
        """Signal that `piece` is available from `peer`."""
        self._piece_queue.add_to_have(peer, piece)

    def get_piece(self, peer: metadata.Peer) -> Optional[metadata.Piece]:
        """Return a piece to download from `peer`, if there is one, and `None`
        otherwise."""
        return self._piece_queue.get_nowait(peer)

    def piece_done(self, peer: metadata.Peer, piece: metadata.Piece, data: bytes):
        """Signal that `data` was received and verified for `piece`."""
        self._file_writer.put_nowait(piece, data)
        self._piece_queue.task_done(peer, piece)


class FileWriter(actor.Actor):
    def __init__(
        self,
        download: Download,
        metainfo: metadata.Metainfo,
        available_pieces: Set[metadata.Piece],
    ):
        super().__init__()

        self._download = download
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

            # Print a counter and progress bar.
            n = len(self._metainfo.pieces)
            i = n - len(self._outstanding)
            digits = len(str(n))
            # Right-align the number of downloaded pieces in a cell of width
            # `digits`, so that the components never move.
            progress = f"Download progress: {i : >{digits}}/{n} pieces."
            width, _ = os.get_terminal_size()
            # Number of parts that the progress bar is split up into. Reserve
            # one character for each of the left and right deliminators, and
            # one space on each side.
            parts = width - len(progress) - 4
            if parts < 10:
                print("\r\x1b[K" + progress, end="")
            else:
                # The number of cells of the progress bar to fill up.
                done = parts * i // n
                # Left-align the filled-up cells.
                bar = f"[{done * '#' : <{parts}}]"
                print("\r\x1b[K" + progress + " " + bar + " ", end="")

        self._download.done()

    ### Queue interface

    def put_nowait(self, piece: metadata.Piece, data: bytes):
        """Signal that `data` was received and verified for `piece`."""
        self._piece_data.put_nowait((piece, data))


class PieceQueue(actor.Actor):
    def __init__(
        self,
        download: Download,
        metainfo: metadata.Metainfo,
        available_pieces: Set[metadata.Piece],
    ):
        super().__init__()

        self._download = download

        # `self._available[peer]` is the set of pieces that `peer` has.
        self._available = collections.defaultdict(set)
        # `self._borrowers[piece]` is the set of peers that `piece` is being
        # downloaded from.
        self._borrowers = collections.defaultdict(set)
        self._outstanding = set(metainfo.pieces) - available_pieces

    def set_have(self, peer: metadata.Peer, pieces: Iterable[metadata.Piece]):
        """Signal that elements of `pieces` are available from `peer`."""
        self._available[peer] = set(pieces)

    def add_to_have(self, peer: metadata.Peer, piece: metadata.Piece):
        """Signal that `piece` is available from `peer`."""
        self._available[peer].add(piece)

    def drop_peer(self, peer: metadata.Peer):
        """Signal that `peer` was dropped."""
        self._available.pop(peer)
        for piece, borrowers in list(self._borrowers.items()):
            borrowers.discard(peer)
            if not borrowers:
                self._borrowers.pop(piece)
                self._outstanding.add(piece)

    ### Queue interface

    def get_nowait(self, peer: metadata.Peer) -> Optional[metadata.Piece]:
        """Return a piece to download from `peer`, if there is one, and `None`
        otherwise."""
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

    def task_done(self, peer: metadata.Peer, piece: metadata.Piece):
        """Signal that `piece` was downloaded from `peer`."""
        if piece not in self._borrowers:
            return
        borrowers = self._borrowers.pop(piece)
        self._download.cancel_piece(borrowers - {peer}, piece)


class PeerConnection(actor.Actor):
    def __init__(
        self,
        download: Download,
        metainfo: metadata.Metainfo,
        tracker_params: metadata.TrackerParameters,
        peer: metadata.Peer,
        *,
        max_requests: int = 10,
    ):
        super().__init__()

        self._download = download
        self._metainfo = metainfo
        self._tracker_params = tracker_params

        self._peer = peer

        self._reader = None
        self._writer = None
        self._unchoked = asyncio.Event()

        self._block_queue = None
        self._slots = asyncio.Semaphore(max_requests)
        self._timer = {}

    async def _read_peer_message(self):
        len_prefix = int.from_bytes(await self._reader.readexactly(4), "big")
        message = await self._reader.readexactly(len_prefix)
        return peer_protocol.message_type(message), message[1:]

    async def _connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._peer.address, self._peer.port
        )

        message = peer_protocol.handshake(
            self._tracker_params.info_hash, self._tracker_params.peer_id
        )
        self._writer.write(message)
        await self._writer.drain()

        response = await self._reader.readexactly(68)
        if not peer_protocol.valid_handshake(response, self._tracker_params.info_hash):
            raise ConnectionError("Peer sent invalid handshake.")

        # TODO: Accept peers that don't send a bitfield.
        while True:
            message_type, payload = await self._read_peer_message()
            if message_type == peer_protocol.PeerMessage.BITFIELD:
                break
            elif message_type == peer_protocol.PeerMessage.EXTENSION_PROTOCOL:
                continue
            else:
                raise ConnectionError("Peer didn't send a bitfield.")

        pieces = peer_protocol.parse_bitfield(payload, self._metainfo.pieces)
        self._download.set_have(self._peer, pieces)

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

    async def _receive_blocks(self):
        while True:
            message_type, payload = await self._read_peer_message()
            if message_type == peer_protocol.PeerMessage.CHOKE:
                self._unchoked.clear()
            elif message_type == peer_protocol.PeerMessage.UNCHOKE:
                self._unchoked.set()
            elif message_type == peer_protocol.PeerMessage.HAVE:
                piece = peer_protocol.parse_have(payload, self._metainfo.pieces)
                self._download.add_to_have(self._peer, piece)
            elif message_type == peer_protocol.PeerMessage.BLOCK:
                block, data = peer_protocol.parse_block(payload, self._metainfo.pieces)
                if block not in self._timer:
                    continue
                self._timer.pop(block).cancel()
                self._slots.release()
                self._block_queue.task_done(block, data)

    async def _timeout(self, block, *, timeout=10):
        await asyncio.sleep(timeout)
        self._crash(TimeoutError("Request timed out."))

    async def _request_blocks(self):
        while True:
            await self._unchoke()
            await self._slots.acquire()
            block = await self._block_queue.get()
            if block is None:
                break
            self._writer.write(peer_protocol.request(block))
            await self._writer.drain()
            self._timer[block] = asyncio.create_task(self._timeout(block))

    ### Actor implementation

    async def _main_coro(self):
        await self._connect()
        self._block_queue = BlockQueue(self)
        await self.spawn_child(self._block_queue)
        await asyncio.gather(self._receive_blocks(), self._request_blocks())

    async def _on_stop(self):
        await self._disconnect()

    ### Messages from Download

    def cancel_piece(self, piece: metadata.Piece):
        """Signal that `piece` does not need to be downloaded anymore."""
        self._block_queue.cancel_piece(piece)

    ### Messages from BlockQueue

    async def cancel_block(self, block: metadata.Block):
        self._writer.write(peer_protocol.cancel(block))
        await self._writer.drain()

    def get_piece(self) -> Optional[metadata.Piece]:
        """Return a piece to download, if there is one, and `None` otherwise."""
        return self._download.get_piece(self._peer)

    def piece_done(self, piece: metadata.Piece, data: bytes):
        """Signal that `data` was received (and verified) for `piece`."""
        self._download.piece_done(self._peer, piece, data)


class BlockQueue(actor.Actor):
    def __init__(self, peer_connection: PeerConnection):
        super().__init__()

        self._peer_connection = peer_connection

        # Because requests are pipelined, we can be requesting blocks from
        # multiple pieces at the same time. However, as we request the blocks
        # in order, the stack always consists of blocks belonging to the newest
        # piece only.
        self._stack = []
        self._outstanding = {}
        self._data = {}

    def _add(self, piece):
        blocks = metadata.blocks(piece)
        self._stack = blocks[::-1]
        self._outstanding[piece] = set(blocks)
        self._data[piece] = {}

    def _remove(self, piece):
        if piece in self._outstanding:
            # If `piece` is the newest piece, discard the stack; otherwise
            # there is nothing to do.
            if self._stack and self._stack[-1].piece == piece:
                self._stack = []
            # TODO: Send cancel messages to the peer.
            self._outstanding.pop(piece)
            return self._data.pop(piece)

    ### Queue interface

    async def get(self) -> Optional[metadata.Block]:
        """Return a block to request, if there is one, and `None` otherwise."""
        if not self._stack:
            piece = self._peer_connection.get_piece()
            if piece is None:
                return None
            self._add(piece)
        return self._stack.pop()

    def task_done(self, block: metadata.Block, data: bytes):
        """Signal that `data` was received for `block`. Raises `ValueError` if
        the length or SHA1 digest of `data` doesn't match with the information
        from the metainfo file."""
        piece = block.piece
        if piece not in self._outstanding or block not in self._outstanding[piece]:
            return
        self._outstanding[piece].remove(block)
        self._data[piece][block] = data
        if not self._outstanding[piece]:
            block_to_data = self._remove(piece)
            data = b"".join(block_to_data[block] for block in sorted(block_to_data))
            if len(data) == piece.length and hashlib.sha1(data).digest() == piece.hash:
                self._peer_connection.piece_done(piece, data)
            else:
                raise ValueError(f"Peer sent invalid data.")

    ### Messages from PeerConnection

    def cancel_piece(self, piece: metadata.Piece):
        """Signal that `piece` does not need to be downloaded anymore."""
        self._remove(piece)
