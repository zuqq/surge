"""Implementation of the metadata exchange protocol.

Specification: [BEP 0009]

The metadata exchange protocol facilities metadata exchange between peers. It
uses the extension protocol to transmit its messages as part of a BitTorrent
connection.

Typical message flow:

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

[BEP 0009]: http://bittorrent.org/beps/bep_0009.html
"""
from typing import Iterable

import asyncio
import dataclasses

from . import _info
from . import _transducer
from .. import tracker
from ..actor import Actor
from ..stream import Stream


__all__ = ("download",)


async def download(
    announce_list: Iterable[str], params: tracker.Parameters, max_peers: int
) -> bytes:
    async with Root(announce_list, params, max_peers) as root:
        return await root.result


class Root(Actor):
    def __init__(
        self, announce_list: Iterable[str], params: tracker.Parameters, max_peers: int
    ):
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

    def done(self, raw_info: bytes) -> None:
        if not self.result.done():
            self.result.set_result(_info.assemble(self._announce_list, raw_info))


class Node(Actor):
    def __init__(self, parent: Root, params: tracker.Parameters, peer: tracker.Peer):
        super().__init__(parent)
        self._coros.add(self._main(params.info_hash, params.peer_id))

        self.peer = peer

    def __repr__(self):
        class_name = self.__module__ + "." + self.__class__.__qualname__
        peer = dataclasses.astuple(self.peer)
        return f"<{class_name} with peer={peer}>"

    async def _main(self, info_hash, peer_id):
        async with Stream(self.peer) as stream:
            transducer = _transducer.mex(info_hash, peer_id)
            try:
                message = None
                while True:
                    event = transducer.send(message)
                    message = None
                    if isinstance(event, _transducer.Write):
                        await stream.write(event.message)
                    elif isinstance(event, _transducer.NeedHandshake):
                        message = await asyncio.wait_for(stream.read_handshake(), 30)
                    elif isinstance(event, _transducer.NeedMessage):
                        message = await asyncio.wait_for(stream.read(), 30)
            except StopIteration as exc:
                self.parent.done(exc.value)
