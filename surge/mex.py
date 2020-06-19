"""Metadata exchange protocol (BEP 9)

Because this protocol is essentially linear, I decided to use a simple
procedural style. The minimal message flow looks like this:

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
     |         Metadata         |
     |<-------------------------|
     |     MetadataRequest      |
     |------------------------->|
     |           ...            |

After exchanging handshakes, the peer might send us BitTorrent messages that are
irrelevant to this particular protocol; such messages are simply ignored.
"""

from typing import Iterable, List

import asyncio
import hashlib

from . import bencoding
from . import messages
from . import tracker
from .actor import Actor
from .stream import Stream


# Metadata ---------------------------------------------------------------------


def valid_info(info_hash: bytes, raw_info: bytes) -> bool:
    return hashlib.sha1(raw_info).digest() == info_hash


def assemble(announce_list: List[str], raw_info: bytes) -> bytes:
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


# Actors -----------------------------------------------------------------------


async def download(announce_list, params):
    async with Root(announce_list, params) as root:
        return await root


class Root(Actor):
    def __init__(self, announce_list: Iterable[str], params: tracker.Parameters):
        super().__init__()

        peer_queue = tracker.PeerQueue(self, announce_list, params)
        self.children.add(peer_queue)
        self._coros.add(self._main(params, peer_queue))

        self._announce_list = announce_list

        self._future = asyncio.get_event_loop().create_future()
        self._slots = asyncio.Semaphore(50)

    def __await__(self):
        return self._future.__await__()

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
        self._future.set_result(assemble(self._announce_list, raw_info))


class Node(Actor):
    def __init__(self, parent: Root, params: tracker.Parameters, peer: tracker.Peer):
        super().__init__(parent)

        self._coros.add(self._main(params.info_hash, params.peer_id))

        self.peer = peer

    async def _main(self, info_hash, peer_id):
        async with Stream(self.peer) as stream:
            await stream.write(messages.Handshake(info_hash, peer_id))

            message = await stream.read_handshake()
            if not isinstance(message, messages.Handshake):
                raise ConnectionError("Expected handshake.")
            if message.info_hash != info_hash:
                raise ConnectionError("Peer's info_hash doesn't match.")

            await stream.write(messages.extension_handshake())

            while True:
                message = await stream.read_message()
                if isinstance(message, messages.ExtensionHandshake):
                    ut_metadata = message.ut_metadata
                    metadata_size = message.metadata_size
                    break

            # The metadata is partitioned into pieces of size `2 ** 14`, except
            # for the last piece which may be smaller. The peer knows this
            # partition, so we only need to tell it the indices of the pieces
            # that we want. Because the total number of pieces is typically very
            # small, a simple stop-and-wait protocol is fast enough.
            piece_length = 2 ** 14
            pieces = []
            for i in range((metadata_size + piece_length - 1) // piece_length):
                await stream.write(messages.metadata_request(i, ut_metadata))
                while True:
                    message = await stream.read_message()
                    if isinstance(message, messages.Metadata) and message.index == i:
                        pieces.append(message.data)
                        break

            raw_info = b"".join(pieces)
            if valid_info(info_hash, raw_info):
                self.parent.done(raw_info)
            else:
                raise ConnectionError("Peer sent invalid data.")
