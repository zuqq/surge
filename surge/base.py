from __future__ import annotations
from typing import DefaultDict, Dict, List, Set

import asyncio
import collections
import functools
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
                 max_peers: int):
        super().__init__()

        self._meta = meta
        self._params = params
        self._outstanding = outstanding
        self._borrowers: DefaultDict[metadata.Piece, Set[PeerConnection]]
        self._borrowers = collections.defaultdict(set)

        self._peer_queue = tracker.PeerQueue(self, params, meta.announce_list)

        self._max_peers = max_peers
        self._peer_connection_slots = asyncio.Semaphore(max_peers)
        self._piece_data = asyncio.Queue()  # type: ignore

        self._print_event = asyncio.Event()
        self._print_event.set()

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
            connection = PeerConnection(self, self._meta, self._params, peer)
            await self.spawn_child(connection)
            self._print_event.set()

    async def _write_pieces(self):
        while self._outstanding:
            piece, data = await self._piece_data.get()
            if piece not in self._outstanding:
                continue
            for chunk in metadata.chunks(self._meta.files, piece):
                path = os.path.join(self._meta.folder, chunk.file.path)
                async with aiofiles.open(path, "rb+") as f:
                    await f.seek(chunk.begin - chunk.file.begin)
                    begin = chunk.begin - piece.begin
                    await f.write(data[begin : begin + chunk.length])
            self._outstanding.remove(piece)
            self._print_event.set()
        self.set_result(None)

    async def _print_progress(self):
        peers_digits = len(str(self._max_peers))
        pieces = len(self._meta.pieces)
        pieces_digits = len(str(pieces))

        while self._outstanding:
            await self._print_event.wait()
            self._print_event.clear()

            outstanding = pieces - len(self._outstanding)
            progress = (
                "Downloading from"
                f" {len(self.children) - 1 : >{peers_digits}} peers:"
                f" {outstanding : >{pieces_digits}}/{pieces} pieces"
            )
            width, _ = os.get_terminal_size()
            parts = width - len(progress) - 4
            if parts < 10:
                print("\r\x1b[K" + progress, end="")
            else:
                bar = f"[{(parts * outstanding // pieces) * '#' : <{parts}}]"
                print("\r\x1b[K" + progress + " " + bar + " ", end="")
        print("\n", end="")

    async def _main(self):
        await self.spawn_child(self._peer_queue)
        await asyncio.gather(
            self._spawn_peer_connections(),
            self._write_pieces(),
            self._print_progress()
        )

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            for piece, borrowers in list(self._borrowers.items()):
                borrowers.discard(child)
                if not borrowers:
                    self._borrowers.pop(piece)
            self._peer_connection_slots.release()
            self._print_event.set()
        else:
            raise RuntimeError(f"Uncaught crash in {child}.")

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


class PeerConnection(actor.Actor):
    def __init__(self,
                 parent: Download,
                 meta: metadata.Metadata,
                 params: tracker.Parameters,
                 peer: tracker.Peer,
                 *,
                 max_requests: int = 10):
        super().__init__(parent)

        self._meta = meta
        self._params = params
        self._peer = peer

        self._stream = None

        self._slots = asyncio.Semaphore(max_requests)
        self._timer: Dict[metadata.Block, asyncio.Task] = {}

        self._stack: List[metadata.Block] = []
        self._outstanding: Dict[metadata.Piece, Set[metadata.Block]] = {}
        self._data: Dict[metadata.Piece, bytearray] = {}

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
            block, data = await self._stream.receive()
            if block not in self._timer:
                continue
            self._timer.pop(block).cancel()
            piece = block.piece
            try:
                self._outstanding[piece].remove(block)
            except KeyError:
                continue
            self._data[piece][block.begin : block.begin + block.length] = data
            if not self._outstanding[piece]:
                piece_data = bytes(self._pop(piece))
                if metadata.valid(piece, piece_data):
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
                piece = self.parent.get_piece(self, self._stream.available)
                if piece is None:
                    break
                blocks = list(metadata.blocks(piece))
                self._stack = blocks[::-1]
                self._outstanding[piece] = set(blocks)
                self._data[piece] = bytearray(piece.length)
            block = self._stack.pop()
            await self._stream.request(block)
            self._timer[block] = asyncio.create_task(self._timeout())

    async def _main(self):
        loop = asyncio.get_running_loop()
        self._stream = protocol.BaseStream(
            self._params.info_hash,
            self._params.peer_id,
            self._meta.pieces
        )
        _, _ = await loop.create_connection(
            functools.partial(
                protocol.Protocol,
                self._stream
            ),
            self._peer.address,
            self._peer.port,
        )
        await self._stream.establish()
        await asyncio.gather(self._receive_blocks(), self._request_blocks())

    async def _on_stop(self):
        if self._stream is not None:
            await self._stream.close()

    ### Messages from Download

    def cancel_piece(self, piece: metadata.Piece):
        # TODO: Send cancel messages to the peer.
        try:
            self._pop(piece)
        except KeyError:
            pass
