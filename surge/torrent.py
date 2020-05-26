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
from . import tracker


class Download(actor.Supervisor):
    """Root node of this module's actor tree.

    Children:
        - One instance of `FileWriter`
        - One instance of `PeerQueue`
        - One instance of `PieceQueue`
        - Up to `max_peers` instances of `PeerConnection`

    Messages between the `PeerConnection` instances and the other actors are
    routed through this class, because an actor should only hold references to
    its parent and children.
    """

    def __init__(
            self,
            metainfo: metadata.Metainfo,
            tracker_params: tracker.Parameters,
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
            self._piece_queue.drop_peer(child.peer)
            self._peer_to_connection.pop(child.peer)
            self._peer_connection_slots.release()
        else:
            self._crash(RuntimeError(f"Irreplaceable actor {repr(child)} crashed."))

    ### Messages from FileWriter

    def done(self):
        self._done.set()

    ### Messages from PieceQueue

    def cancel_piece(self, peers: Iterable[tracker.Peer], piece: metadata.Piece):
        for peer in peers:
            self._peer_to_connection[peer].cancel_piece(piece)

    ### Messages from PeerConnection

    def set_have(self, peer: tracker.Peer, pieces: Iterable[metadata.Piece]):
        self._piece_queue.set_have(peer, pieces)

    def add_to_have(self, peer: tracker.Peer, piece: metadata.Piece):
        self._piece_queue.add_to_have(peer, piece)

    def get_piece(self, peer: tracker.Peer) -> Optional[metadata.Piece]:
        return self._piece_queue.get_nowait(peer)

    def piece_done(self, peer: tracker.Peer, piece: metadata.Piece, data: bytes):
        self._file_writer.put_nowait(piece, data)
        self._piece_queue.task_done(peer, piece)


class FileWriter(actor.Actor):
    """Writes downloaded pieces to the file system.

    Downloaded pieces are supplied via the method `put_nowait` and then written
    to the file system with `aiofiles`.
    """

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

    def _print_progress(self):
        # Print a counter and progress bar.
        n = len(self._metainfo.pieces)
        i = n - len(self._outstanding)
        digits = len(str(n))
        # Right-align the number of downloaded pieces in a cell of width
        # `digits`, so that the components never move.
        progress = f"Download progress: {i : >{digits}}/{n} pieces."
        width, _ = os.get_terminal_size()
        # Number of parts that the progress bar is split up into. Reserve one
        # character for each of the left and right delimiters, and one space on
        # each side.
        parts = width - len(progress) - 4
        if parts < 10:
            print("\r\x1b[K" + progress, end="")
        else:
            # The number of cells of the progress bar to fill up.
            done = parts * i // n
            # Left-align the filled-up cells.
            bar = f"[{done * '#' : <{parts}}]"
            print("\r\x1b[K" + progress + " " + bar + " ", end="")

    async def _main_coro(self):
        piece_to_chunks = metadata.piece_to_chunks(
            self._metainfo.pieces, self._metainfo.files
        )
        self._print_progress()
        while self._outstanding:
            piece, data = await self._piece_data.get()
            if piece not in self._outstanding:
                continue
            # Write the piece to the file system by writing each of its chunks
            # to the corresponding file.
            for c in piece_to_chunks[piece]:
                file_path = os.path.join(self._metainfo.folder, c.file.path)
                async with aiofiles.open(file_path, "rb+") as f:
                    await f.seek(c.file_offset)
                    await f.write(data[c.piece_offset : c.piece_offset + c.length])
            self._outstanding.remove(piece)
            self._print_progress()
        self._download.done()

    ### Queue interface

    def put_nowait(self, piece: metadata.Piece, data: bytes):
        self._piece_data.put_nowait((piece, data))


class PieceQueue(actor.Actor):
    """Tracks piece availability.

    More precisely, this class keeps track of which pieces the connected peers
    have and which pieces are being downloaded from them.

    The parent `Download` instance calls `get_nowait` to get pieces to download;
    successful downloads are reported via `task_done`.
    """
    def __init__(
            self,
            download: Download,
            metainfo: metadata.Metainfo,
            available_pieces: Set[metadata.Piece],
        ):
        super().__init__()

        self._download = download

        # Bookkeeping:
        # - `self._available[peer]` is the set of pieces that `peer` has;
        # - `self._borrowers[piece]` is the set of peers that `piece` is being
        # downloaded from;
        # - `self._outstanding` is the set of pieces that are missing, except
        # those that are being downloaded right now.
        self._available = collections.defaultdict(set)
        self._borrowers = collections.defaultdict(set)
        self._outstanding = set(metainfo.pieces) - available_pieces

    def set_have(self, peer: tracker.Peer, pieces: Iterable[metadata.Piece]):
        self._available[peer] = set(pieces)

    def add_to_have(self, peer: tracker.Peer, piece: metadata.Piece):
        self._available[peer].add(piece)

    def drop_peer(self, peer: tracker.Peer):
        if peer not in self._available:
            return
        self._available.pop(peer)
        for piece, borrowers in list(self._borrowers.items()):
            borrowers.discard(peer)
            if not borrowers:
                self._borrowers.pop(piece)
                self._outstanding.add(piece)

    ### Queue interface

    def get_nowait(self, peer: tracker.Peer) -> Optional[metadata.Piece]:
        """Return a piece to download from `peer`."""
        # If there are missing pieces that are not being downloaded right now,
        # choose from those. Otherwise we choose from all missing pieces.
        pool = self._outstanding or set(self._borrowers)
        if not pool:
            return None
        # Try to return a missing piece that the peer has.
        available = pool & self._available[peer]
        if available:
            piece = random.choice(list(available))
        # If there are none, fall back to a random missing piece.
        else:
            piece = random.choice(list(pool))
        pool.remove(piece)
        self._borrowers[piece].add(peer)
        return piece

    def task_done(self, peer: tracker.Peer, piece: metadata.Piece):
        """Mark `piece` as downloaded.

        If `piece` is also being downloaded from peers other than `peer`,
        instruct the corresponding `PeerConnection` instances to cancel the
        download.
        """
        if piece not in self._borrowers:
            return
        borrowers = self._borrowers.pop(piece)
        self._download.cancel_piece(borrowers - {peer}, piece)


class PeerConnection(actor.Actor):
    """Encapsulates the connection with a peer.

    Children:
        - One instance of `BlockQueue`.
    """
    def __init__(
            self,
            download: Download,
            metainfo: metadata.Metainfo,
            tracker_params: tracker.Parameters,
            peer: tracker.Peer,
            *,
            max_requests: int = 10,
        ):
        super().__init__()

        self._download = download
        self._metainfo = metainfo
        self._tracker_params = tracker_params

        self.peer = peer

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
            self.peer.address, self.peer.port
        )

        message = peer_protocol.handshake(
            self._tracker_params.info_hash, self._tracker_params.peer_id
        )
        self._writer.write(message)
        await self._writer.drain()

        # TODO: Validate the peer's handshake.
        _ = await self._reader.readexactly(68)

        # TODO: Accept peers that don't send a bitfield.
        while True:
            message_type, payload = await self._read_peer_message()
            if message_type == peer_protocol.Message.BITFIELD:
                break
            if message_type == peer_protocol.Message.EXTENSION_PROTOCOL:
                continue
            raise ConnectionError("Peer didn't send a bitfield.")

        pieces = peer_protocol.parse_bitfield(payload, self._metainfo.pieces)
        self._download.set_have(self.peer, pieces)

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
            if message_type == peer_protocol.Message.CHOKE:
                self._unchoked.clear()
            elif message_type == peer_protocol.Message.UNCHOKE:
                self._unchoked.set()
            elif message_type == peer_protocol.Message.HAVE:
                piece = peer_protocol.parse_have(payload, self._metainfo.pieces)
                self._download.add_to_have(self.peer, piece)
            elif message_type == peer_protocol.Message.BLOCK:
                block, data = peer_protocol.parse_block(payload, self._metainfo.pieces)
                if block not in self._timer:
                    continue
                self._timer.pop(block).cancel()
                self._slots.release()
                self._block_queue.task_done(block, data)

    async def _timeout(self, *, timeout=10):
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
            self._timer[block] = asyncio.create_task(self._timeout())

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
        self._block_queue.cancel_piece(piece)

    ### Messages from BlockQueue

    async def cancel_block(self, block: metadata.Block):
        self._writer.write(peer_protocol.cancel(block))
        await self._writer.drain()

    def get_piece(self) -> Optional[metadata.Piece]:
        return self._download.get_piece(self.peer)

    def piece_done(self, piece: metadata.Piece, data: bytes):
        self._download.piece_done(self.peer, piece, data)


class BlockQueue(actor.Actor):
    """Supplies fresh blocks and keeps track of downloaded ones.

    This class exposes a queue interface that the parent `PeerConnection`
    instance uses to obtain blocks to request from the peer (by calling `get`)
    and later return the blocks and the received data (by calling `task_done`).

    In order to supply a continuous stream of blocks, a new piece is started as
    soon as all blocks from the last one are being requested.

    Once all blocks belonging to a piece have been downloaded, the data is
    checked against the corresponding hash from the metainfo file; if the hashes
    match up, the piece and its data are sent to the parent.
    """
    def __init__(self, peer_connection: PeerConnection):
        super().__init__()

        self._peer_connection = peer_connection

        # Bookkeeping:
        # - `self._stack` is a list of the newest piece's blocks that have not
        # been requested yet;
        # - `self._outstanding[piece]` is the set of blocks of `piece` that
        # still need to be downloaded;
        # - `self._data[block.piece][block]` is the data that was received
        # for `block`.
        self._stack = []
        self._outstanding = {}
        self._data = {}

    def _pop(self, piece):
        # If `piece` is the newest piece, discard the stack.
        if self._stack and self._stack[-1].piece == piece:
            self._stack = []
        self._outstanding.pop(piece)
        return self._data.pop(piece)

    ### Queue interface

    async def get(self) -> Optional[metadata.Block]:
        """Return a block to download from the peer."""
        if not self._stack:
            piece = self._peer_connection.get_piece()
            if piece is None:
                return None
            blocks = metadata.blocks(piece)
            self._stack = blocks[::-1]
            self._outstanding[piece] = set(blocks)
            self._data[piece] = {}
        return self._stack.pop()

    def task_done(self, block: metadata.Block, data: bytes):
        """Mark `block` as downloaded and supply the received data.

        If `block` was the last outstanding block of `block.piece`, check the
        piece's data. If the check passes, forward the piece and its data to the
        parent; else raise `ValueError`.
        """
        piece = block.piece
        if piece not in self._outstanding or block not in self._outstanding[piece]:
            return
        self._outstanding[piece].remove(block)
        self._data[piece][block] = data
        if not self._outstanding[piece]:
            block_to_data = self._pop(piece)
            data = b"".join(block_to_data[block] for block in sorted(block_to_data))
            if len(data) == piece.length and hashlib.sha1(data).digest() == piece.hash:
                self._peer_connection.piece_done(piece, data)
            else:
                raise ValueError("Peer sent invalid data.")

    ### Messages from PeerConnection

    def cancel_piece(self, piece: metadata.Piece):
        # TODO: Send cancel messages to the peer.
        try:
            self._pop(piece)
        except KeyError:
            pass
