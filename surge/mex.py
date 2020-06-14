from typing import Iterable

import asyncio
import functools
import hashlib

from . import actor
from . import protocol
from . import tracker


class Download(actor.Supervisor):
    def __init__(self,
                 params: tracker.Parameters,
                 announce_list: Iterable[str],
                 *,
                 max_peers: int = 10):
        super().__init__()

        self._params = params
        self._peer_queue = tracker.PeerQueue(announce_list, params)
        self._peer_connection_slots = asyncio.Semaphore(max_peers)

    ### Actor implementation

    async def _main(self):
        await self.spawn_child(self._peer_queue)
        while True:
            await self._peer_connection_slots.acquire()
            peer = await self._peer_queue.get()
            await self.spawn_child(PeerConnection(self._params, peer))

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            self._peer_connection_slots.release()
        else:
            raise RuntimeError(f"Uncaught crash in {child}.")


class PeerConnection(actor.Actor):
    def __init__(self, params: tracker.Parameters, peer: tracker.Peer):
        super().__init__()

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
        metadata_size = (await self._stream.establish()).metadata_size

        data = []
        for i in range((metadata_size + 2 ** 14 - 1) // 2 ** 14):
            await self._stream.request(i)
            index, payload = await self._stream.receive()
            if index == i:
                data.append(payload)
        raw_info = b"".join(data)
        if hashlib.sha1(raw_info).digest() != self._params.info_hash:
            raise ConnectionError("Peer sent invalid data.")
        self.parent.set_result(raw_info)

    async def _on_stop(self):
        if self._stream is not None:
            await self._stream.close()
