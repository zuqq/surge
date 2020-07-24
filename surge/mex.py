"""Metadata Exchange Protocol (BEP 9)

Minimal message flow:

    Us                         Peer
     |        Handshake         |
     |------------------------->|
     |        Handshake         |
     |<-------------------------|
     |    ExtensionHandshake    |
     |------------------------->|
     |    ExtensionHandshake    |
     |<-------------------------|
     |     MetadataRequest      |
     |------------------------->|
     |       MetadataData       |
     |<-------------------------|
     |     MetadataRequest      |
     |------------------------->|
     |           ...            |

After exchanging handshakes, the peer might send us BitTorrent messages that are
irrelevant to this particular protocol; such messages are simply ignored.
"""

from typing import Iterable

import asyncio
import dataclasses
import hashlib

from . import bencoding
from . import messages
from . import tracker
from .actor import Actor
from .stream import Stream


# Metadata ---------------------------------------------------------------------


def valid_info(info_hash: bytes, raw_info: bytes) -> bool:
    return hashlib.sha1(raw_info).digest() == info_hash


def assemble(announce_list: Iterable[str], raw_info: bytes) -> bytes:
    # We can't just decode and re-encode, because the value associated with
    # the key `b"info"` needs to be preserved exactly.
    return b"".join(
        (
            b"d",
            b"13:announce-list",
            bencoding.encode([[url.encode() for url in announce_list]]),
            b"4:info",
            raw_info,
            b"e",
        )
    )


# Protocol ---------------------------------------------------------------------


def mex(info_hash: bytes, peer_id: bytes):
    received = yield messages.Handshake(info_hash, peer_id, extension_protocol=True)
    if not isinstance(received, messages.Handshake):
        raise ConnectionError("Expected handshake.")
    if received.info_hash != info_hash:
        raise ConnectionError("Wrong info_hash.")

    message = messages.extension_handshake()
    while True:
        received = yield message
        message = None
        if isinstance(received, messages.ExtensionHandshake):
            ut_metadata = received.ut_metadata
            metadata_size = received.metadata_size
            break

    # The metadata is partitioned into pieces of size `2 ** 14`, except for the
    # last piece which may be smaller. The peer knows this partition, so we only
    # need to tell it the indices of the pieces that we want. Because the total
    # number of pieces is typically very small, a simple stop-and-wait protocol
    # is fast enough.
    piece_length = 2 ** 14
    pieces = []
    for i in range((metadata_size + piece_length - 1) // piece_length):
        message = messages.metadata_request(i, ut_metadata)
        while True:
            received = yield message
            message = None
            if isinstance(received, messages.MetadataData):
                # We assume that the peer sends us data for the piece that we
                # just requested; if not, the result of the transaction will be
                # invalid. This assumption is reasonable because we request one
                # piece at a time.
                pieces.append(received.data)
                break

    raw_info = b"".join(pieces)
    if valid_info(info_hash, raw_info):
        return raw_info
    raise ConnectionError("Invalid data.")


# Actors -----------------------------------------------------------------------


async def download(announce_list, params, max_peers):
    async with Root(announce_list, params, max_peers) as root:
        return await root.result


class Root(Actor):
    def __init__(
            self,
            announce_list: Iterable[str],
            params: tracker.Parameters,
            max_peers: int):
        super().__init__()
        peer_queue = tracker.PeerQueue(self, announce_list, params)
        self.children.add(peer_queue)
        self._coros.add(self._main(params, peer_queue))

        self.result = asyncio.get_event_loop().create_future()
        self._announce_list = announce_list
        self._slots = asyncio.Semaphore(max_peers)

    async def _main(self, params, peer_queue):
        while True:
            await self._slots.acquire()
            peer = await peer_queue.get()
            await self.spawn_child(Node(self, params, peer))

    def _on_child_crash(self, child):
        if isinstance(child, Node):
            self._slots.release()
        else:
            super()._on_child_crash(child)

    def done(self, raw_info: bytes):
        if not self.result.done():
            self.result.set_result(assemble(self._announce_list, raw_info))


class Node(Actor):
    def __init__(self, parent: Root, params: tracker.Parameters, peer: tracker.Peer):
        super().__init__(parent)
        self._coros.add(self._main(params.info_hash, params.peer_id))

        self.peer = peer

    def __repr__(self):
        class_name = self.__module__ + '.' + self.__class__.__qualname__
        peer = dataclasses.astuple(self.peer)
        return f"<{class_name} with peer={peer}>"

    async def _main(self, info_hash, peer_id):
        async with Stream(self.peer) as stream:
            state = mex(info_hash, peer_id)

            message = state.send(None)
            await stream.write(message)
            received = await stream.read_handshake()

            try:
                while True:
                    message = state.send(received)
                    if message is not None:
                        await stream.write(message)
                    received = await asyncio.wait_for(stream.read(), 5)
            except StopIteration as exc:
                self.parent.done(exc.value)
