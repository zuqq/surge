from typing import Iterable

import asyncio
import functools
import hashlib

from . import actor
from . import protocol
from . import tracker


def pieces(metadata_size):
    piece_size = 2 ** 14
    return range((metadata_size + piece_size - 1) // piece_size)


def valid(info_hash, metadata_size, raw_info):
    return (len(raw_info) == metadata_size
            and hashlib.sha1(raw_info).digest() == info_hash)


class Download(actor.Supervisor):
    def __init__(self,
                 params: tracker.Parameters,
                 announce_list: Iterable[str],
                 *,
                 max_peers: int = 10):
        super().__init__()

        self._params = params
        self._peer_queue = tracker.PeerQueue(self, announce_list, params)
        self._peer_connection_slots = asyncio.Semaphore(max_peers)

    ### Actor implementation

    async def _main(self):
        await self.spawn_child(self._peer_queue)
        while True:
            await self._peer_connection_slots.acquire()
            peer = await self._peer_queue.get()
            await self.spawn_child(PeerConnection(self, self._params, peer))

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            self._peer_connection_slots.release()
        else:
            raise RuntimeError(f"Uncaught crash in {child}.")


class PeerConnection(actor.Actor):
    def __init__(self,
                 parent: Download,
                 params: tracker.Parameters,
                 peer: tracker.Peer):
        super().__init__(parent)

        self._params = params
        self._peer = peer
        self._stream = None

    ### Actor implementation

    async def _main(self):
        loop = asyncio.get_running_loop()
        self._stream = protocol.MetadataStream(
            self._params.info_hash,
            self._params.peer_id,
        )
        _, _ = await loop.create_connection(
            functools.partial(
                protocol.Protocol,
                self._stream
            ),
            self._peer.address,
            self._peer.port,
        )
        metadata_size = await self._stream.establish()
        data = []
        for i in pieces(metadata_size):
            await self._stream.request(i)
            index, payload = await self._stream.receive()
            if index == i:
                data.append(payload)
        raw_info = b"".join(data)
        if valid(self._params.info_hash, metadata_size, raw_info):
            self.parent.set_result(raw_info)
        else:
            raise ConnectionError("Peer sent invalid data.")

    async def _on_stop(self):
        if self._stream is not None:
            await self._stream.close()
