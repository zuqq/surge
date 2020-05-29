from typing import Set

import asyncio
import collections
import functools
import hashlib
import os
import random

import aiofiles

from . import actor
from . import metadata
from . import peer_queue
from . import protocol
from . import tracker


class Download(actor.Supervisor):
    def __init__(self,
                 metainfo: metadata.Metainfo,
                 tracker_params: tracker.Parameters,
                 available_pieces: Set[metadata.Piece],
                 *,
                 max_peers: int = 50):
        super().__init__()

        self._metainfo = metainfo
        self._tracker_params = tracker_params
        self._outstanding = set(metainfo.pieces) - available_pieces
        self._borrowers = collections.defaultdict(set)

        metadata.ensure_files_exist(self._metainfo.folder, self._metainfo.files)

        self._printer = None
        self._peer_queue = None

        loop = asyncio.get_event_loop()
        self._done = loop.create_future()

        self._piece_data = asyncio.Queue()
        self._peer_connection_slots = asyncio.Semaphore(max_peers)
        self._peer_to_connection = {}

    async def _spawn_peer_connections(self):
        while True:
            await self._peer_connection_slots.acquire()
            peer = await self._peer_queue.get()
            connection = PeerConnection(self._metainfo, self._tracker_params, peer)
            await self.spawn_child(connection)
            self._peer_to_connection[peer] = connection

    async def _write_pieces(self):
        piece_to_chunks = metadata.piece_to_chunks(
            self._metainfo.pieces,
            self._metainfo.files
        )
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
        self._done.set_result(None)

    async def wait_done(self):
        return await self._done

    ### Actor implementation

    async def _main_coro(self):
        self._peer_queue = peer_queue.PeerQueue(
            self._metainfo.announce_list,
            self._tracker_params
        )
        await self.spawn_child(self._peer_queue)

        self._printer = Printer(len(self._metainfo.pieces), len(self._outstanding))
        await self.spawn_child(self._printer)

        await asyncio.gather(self._spawn_peer_connections(), self._write_pieces())

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            for piece, borrowers in list(self._borrowers.items()):
                borrowers.discard(child.peer)
                if not borrowers:
                    self._borrowers.pop(piece)
                    self._outstanding.add(piece)
            self._peer_to_connection.pop(child.peer)
            self._peer_connection_slots.release()
        else:
            self._crash(RuntimeError(f"Irreplaceable actor {repr(child)} crashed."))

    ### Messages from PeerConnection

    def get_piece(self, peer, available):
        # If there are missing pieces that are not being downloaded right now,
        # choose from those. Otherwise we choose from all missing pieces.
        pool = self._outstanding or set(self._borrowers)
        if not pool:
            return None
        # Try to return a missing piece that the peer has.
        good = pool & available
        if good:
            piece = random.choice(list(good))
        # If there are none, fall back to a random missing piece.
        else:
            piece = random.choice(list(pool))
        self._outstanding.discard(piece)
        self._borrowers[piece].add(peer)
        return piece

    def piece_done(self, peer: tracker.Peer, piece: metadata.Piece, data: bytes):
        if piece not in self._borrowers:
            return
        for borrower in self._borrowers.pop(piece) - {peer}:
            self._peer_to_connection[borrower].cancel_piece(piece)
        self._piece_data.put_nowait((piece, data))
        self._printer.advance()


class Printer(actor.Actor):
    def __init__(self, pieces: int, outstanding: int):
        super().__init__()

        self._pieces = pieces
        self._outstanding = outstanding
        self._event = asyncio.Event()

    async def _main_coro(self):
        while True:
            await self._event.wait()
            self._event.clear()
            # Print a counter and progress bar.
            n = self._pieces
            i = n - self._outstanding
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

    def advance(self):
        self._outstanding -= 1
        self._event.set()


class PeerConnection(actor.Actor):
    def __init__(self,
                 metainfo: metadata.Metainfo,
                 tracker_params: tracker.Parameters,
                 peer: tracker.Peer,
                 *,
                 max_requests: int = 10):
        super().__init__()

        self._metainfo = metainfo
        self._tracker_params = tracker_params

        self.peer = peer
        self._available = set()

        self._protocol = None
        self._slots = asyncio.Semaphore(max_requests)
        self._timer = {}

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

    async def _receive_blocks(self):
        while True:
            block, data = await self._protocol.receive()
            if block not in self._timer:
                continue
            self._timer.pop(block).cancel()
            piece = block.piece
            try:
                self._outstanding[piece].remove(block)
            except KeyError:
                continue
            self._data[piece][block] = data
            if not self._outstanding[piece]:
                block_to_data = self._pop(piece)
                data = b"".join(block_to_data[block] for block in sorted(block_to_data))
                if (len(data) == piece.length
                        and hashlib.sha1(data).digest() == piece.hash):
                    self.parent.piece_done(self.peer, piece, data)
                else:
                    raise ValueError("Peer sent invalid data.")
            self._slots.release()

    async def _timeout(self, *, timeout=10):
        await asyncio.sleep(timeout)
        self._crash(TimeoutError("Request timed out."))

    async def _request_blocks(self):
        while True:
            await self._slots.acquire()
            if not self._stack:
                piece = self.parent.get_piece(self.peer, self._available)
                if piece is None:
                    return None
                blocks = metadata.blocks(piece)
                self._stack = blocks[::-1]
                self._outstanding[piece] = set(blocks)
                self._data[piece] = {}
            block = self._stack.pop()
            if block is None:
                break
            await self._protocol.request(block)
            self._timer[block] = asyncio.create_task(self._timeout())

    async def _disconnect(self):
        if self._protocol is None:
            return
        self._protocol.close()
        await self._protocol.wait_closed()

    ### Actor implementation

    async def _main_coro(self):
        loop = asyncio.get_running_loop()
        _, self._protocol = await loop.create_connection(
            functools.partial(
                protocol.Protocol,
                self._tracker_params.info_hash,
                self._tracker_params.peer_id,
                self._metainfo.pieces,
            ),
            self.peer.address,
            self.peer.port,
        )

        # TODO: Validate the peer's handshake.
        _, _, _ = await self._protocol.handshake
        self._available = await self._protocol.bitfield

        await asyncio.gather(self._receive_blocks(), self._request_blocks())

    async def _on_stop(self):
        await self._disconnect()

    ### Messages from Download

    def cancel_piece(self, piece: metadata.Piece):
        # TODO: Send cancel messages to the peer.
        try:
            self._pop(piece)
        except KeyError:
            pass
