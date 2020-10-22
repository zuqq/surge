"""Implementation of the metadata exchange protocol.

Specification: [BEP 0009]

The metadata exchange protocol is a mechanism to exchange metadata (i.e.,
`.torrent` files) with peers. It uses the extension protocol to transmit its
messages as part of a BitTorrent connection; see `messages.MetadataMessage`.
Therefore the implementation uses the same approach as that of the main
protocol.

[BEP 0009]: http://bittorrent.org/beps/bep_0009.html
"""
from typing import Iterable

import asyncio

from . import _info
from . import _transducer
from .. import tracker
from ..actor import Actor
from ..stream import Stream


__all__ = ("download",)


async def download(info_hash: bytes,
                   announce_list: Iterable[str],
                   peer_id: bytes,
                   max_peers: int) -> bytes:
    """Return the content of the `.torrent` file."""
    async with Root(info_hash, announce_list, peer_id, max_peers) as root:
        return await root.result


class Root(Actor):
    def __init__(self,
                 info_hash: bytes,
                 announce_list: Iterable[str],
                 peer_id: bytes,
                 max_peers: int):
        super().__init__()
        peer_queue = tracker.PeerQueue(self, info_hash, announce_list, peer_id)
        self.children.add(peer_queue)
        self._coros.add(self._main(info_hash, peer_id, peer_queue))

        self._announce_list = announce_list
        self._slots = asyncio.Semaphore(max_peers)

        # Future that will hold the metadata.
        self.result = asyncio.get_event_loop().create_future()

    async def _main(self, info_hash, peer_id, peer_queue):
        while True:
            await self._slots.acquire()
            peer = await peer_queue.get()
            await self.spawn_child(Node(self, info_hash, peer_id, peer))

    def _on_child_crash(self, child):
        if isinstance(child, Node):
            self._slots.release()
        else:
            super()._on_child_crash(child)

    def done(self, raw_info: bytes) -> None:
        if not self.result.done():
            self.result.set_result(_info.assemble(self._announce_list, raw_info))


class Node(Actor):
    def __init__(self,
                 parent: Root,
                 info_hash: bytes,
                 peer_id: bytes,
                 peer: tracker.Peer):
        super().__init__(parent)
        self._coros.add(self._main(info_hash, peer_id))

        self.peer = peer

    async def _main(self, info_hash, peer_id):
        async with Stream(self.peer) as stream:
            transducer = _transducer.mex(info_hash, peer_id)
            try:
                message = None
                while True:
                    event = transducer.send(message)
                    message = None
                    if isinstance(event, _transducer.Send):
                        await stream.write(event.message)
                    elif isinstance(event, _transducer.ReceiveHandshake):
                        message = await asyncio.wait_for(stream.read_handshake(), 30)
                    elif isinstance(event, _transducer.ReceiveMessage):
                        message = await asyncio.wait_for(stream.read(), 30)
            except StopIteration as exc:
                self.parent.done(exc.value)
