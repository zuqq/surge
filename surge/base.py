from __future__ import annotations
from typing import DefaultDict, Dict, List, Set

import asyncio
import collections
import functools
import hashlib
import os
import random

import aiofiles  # type: ignore

from . import actor
from . import metadata
from . import protocol
from . import tracker


class Download(actor.Supervisor):
    def __init__(self,
                 meta: metadata.Metadata,
                 params: tracker.Parameters,
                 outstanding: Set[metadata.Piece],
                 *,
                 max_peers: int = 50):
        super().__init__()

        self._meta = meta
        self._params = params
        self._outstanding = outstanding
        self._borrowers: DefaultDict[metadata.Piece, Set[PeerConnection]]
        self._borrowers = collections.defaultdict(set)

        self._peer_queue = tracker.PeerQueue(meta.announce_list, params)
        self._printer = Printer(len(meta.pieces), len(outstanding))

        self._peer_connection_slots = asyncio.Semaphore(max_peers)
        self._piece_data = asyncio.Queue()  # type: ignore

    def __repr__(self):
        cls = self.__class__.__name__
        info = [
            f"info_hash={repr(self._params.info_hash)}",
            f"peer_id={repr(self._params.peer_id)}",
        ]
        return f"<{cls} object at {hex(id(self))} with {', '.join(info)}>"

    ### Actor implementation

    async def _spawn_peer_connections(self):
        while True:
            await self._peer_connection_slots.acquire()
            peer = await self._peer_queue.get()
            connection = PeerConnection(self._meta, self._params, peer)
            await self.spawn_child(connection)

    async def _write_pieces(self):
        chunks = metadata.chunks(self._meta.pieces, self._meta.files)
        while self._outstanding:
            piece, data = await self._piece_data.get()
            if piece not in self._outstanding:
                continue
            for c in chunks[piece]:
                file_path = os.path.join(self._meta.folder, c.file.path)
                async with aiofiles.open(file_path, "rb+") as f:
                    await f.seek(c.file_offset)
                    await f.write(data[c.piece_offset : c.piece_offset + c.length])
            self._outstanding.remove(piece)
            self._printer.advance()
        self.result.set_result(None)

    async def _main(self):
        await self.spawn_child(self._peer_queue)
        await self.spawn_child(self._printer)
        await asyncio.gather(self._spawn_peer_connections(), self._write_pieces())

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            for piece, borrowers in list(self._borrowers.items()):
                borrowers.discard(child)
                if not borrowers:
                    self._borrowers.pop(piece)
            self._peer_connection_slots.release()
        else:
            self._crash(RuntimeError(f"Irreplaceable actor {repr(child)} crashed."))

    ### Messages from PeerConnection

    def get_piece(self,
                  peer_connection: PeerConnection,
                  available: Set[metadata.Piece]):
        borrowed = set(self._borrowers)
        pool = self._outstanding - borrowed or borrowed
        if not pool:
            return None
        try:
            piece = random.choice(list(pool & available))
        except IndexError:
            piece = random.choice(list(pool))
        self._borrowers[piece].add(peer_connection)
        return piece

    def piece_done(self,
                   peer_connection: PeerConnection,
                   piece: metadata.Piece,
                   data: bytes):
        if piece not in self._borrowers:
            return
        for borrower in self._borrowers.pop(piece) - {peer_connection}:
            borrower.cancel_piece(piece)
        self._piece_data.put_nowait((piece, data))


class Printer(actor.Actor):
    def __init__(self, pieces: int, outstanding: int):
        super().__init__()

        self._pieces = pieces
        self._outstanding = outstanding
        self._event = asyncio.Event()

    def __repr__(self):
        cls = self.__class__.__name__
        return f"<{cls} object at {hex(id(self))}>"

    ### Actor implementation

    async def _main(self):
        while True:
            await self._event.wait()
            self._event.clear()

            n = self._pieces
            i = n - self._outstanding
            digits = len(str(n))
            progress = f"Download progress: {i : >{digits}}/{n} pieces."
            width, _ = os.get_terminal_size()
            parts = width - len(progress) - 4
            if parts < 10:
                print("\r\x1b[K" + progress, end="")
            else:
                done = parts * i // n
                bar = f"[{done * '#' : <{parts}}]"
                print("\r\x1b[K" + progress + " " + bar + " ", end="")

    ### Interface

    def advance(self):
        self._outstanding -= 1
        self._event.set()


class PeerConnection(actor.Actor):
    def __init__(self,
                 meta: metadata.Metadata,
                 params: tracker.Parameters,
                 peer: tracker.Peer,
                 *,
                 max_requests: int = 10):
        super().__init__()

        self._meta = meta
        self._params = params
        self._peer = peer

        self._protocol = None

        self._slots = asyncio.Semaphore(max_requests)
        self._timer: Dict[metadata.Block, asyncio.Task] = {}

        self._stack: List[metadata.Block] = []
        self._outstanding: Dict[metadata.Piece, Set[metadata.Block]] = {}
        self._data: Dict[metadata.Piece, Dict[metadata.Block, bytes]] = {}

    def __repr__(self):
        cls = self.__class__.__name__
        peer = f"peer={self._peer}"
        return f"<{cls} object at {hex(id(self))} with {peer}>"

    ### Actor implementation

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
                data = self._pop(piece)
                piece_data = b"".join(data[block] for block in sorted(data))
                if (len(piece_data) == piece.length
                        and hashlib.sha1(piece_data).digest() == piece.hash):
                    self.parent.piece_done(self, piece, piece_data)
                else:
                    raise ValueError("Peer sent invalid data.")
            self._slots.release()

    async def _timeout(self, *, timeout=5):
        await asyncio.sleep(timeout)
        self._crash(TimeoutError("Request timed out."))

    async def _request_blocks(self):
        while True:
            await self._slots.acquire()
            if not self._stack:
                piece = self.parent.get_piece(self, self._protocol.available)
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

    async def _main(self):
        loop = asyncio.get_running_loop()
        _, self._protocol = await loop.create_connection(
            functools.partial(
                protocol.BaseProtocol,
                self._params.info_hash,
                self._params.peer_id,
                self._meta.pieces,
            ),
            self._peer.address,
            self._peer.port,
        )
        # TODO: Validate the peer's handshake.
        await self._protocol.handshake
        await self._protocol.bitfield
        await asyncio.gather(self._receive_blocks(), self._request_blocks())

    async def _on_stop(self):
        if self._protocol is not None:
            await self._protocol.close()

    ### Messages from Download

    def cancel_piece(self, piece: metadata.Piece):
        # TODO: Send cancel messages to the peer.
        try:
            self._pop(piece)
        except KeyError:
            pass
