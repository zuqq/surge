from typing import List

import asyncio
import functools
import hashlib

from . import actor
from . import protocol
from . import tracker


class Download(actor.Supervisor):
    def __init__(
        self,
        tracker_params: tracker.Parameters,
        announce_list: List[str],
        *,
        max_peers: int = 10,
    ):
        super().__init__()

        self._tracker_params = tracker_params
        self._peer_queue = tracker.PeerQueue(announce_list, tracker_params)
        self._done = asyncio.get_event_loop().create_future()
        self._peer_connection_slots = asyncio.Semaphore(max_peers)

    ### Actor implementation

    async def _main(self):
        await self.spawn_child(self._peer_queue)
        while True:
            await self._peer_connection_slots.acquire()
            peer = await self._peer_queue.get()
            await self.spawn_child(PeerConnection(self._tracker_params, peer))

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            self._peer_connection_slots.release()
        else:
            self._crash(RuntimeError(f"Irreplaceable actor {repr(child)} crashed."))

    ### Messages from PeerConnection

    def done(self, raw_metainfo: bytes):
        """Signal that `raw_metainfo` has been received and verified."""
        self._done.set_result(raw_metainfo)

    ### Interface

    async def wait_done(self) -> bytes:
        """Return the raw metainfo file once it has been downloaded."""
        return await self._done


class PeerConnection(actor.Actor):
    def __init__(
        self, tracker_params: tracker.Parameters, peer: tracker.Peer,
    ):
        super().__init__()

        self._tracker_params = tracker_params
        self._peer = peer
        self._protocol = None

    ### Actor implementation

    async def _main(self):
        loop = asyncio.get_running_loop()
        _, self._protocol = await loop.create_connection(
            functools.partial(
                protocol.MetadataProtocol,
                self._tracker_params.info_hash,
                self._tracker_params.peer_id,
            ),
            self._peer.address,
            self._peer.port,
        )

        # TODO: Validate the peer's handshake.
        _ = await self._protocol.handshake

        metadata_size = await self._protocol.metadata_size

        data = []
        for i in range((metadata_size + 2 ** 14 - 1) // 2 ** 14):
            await self._protocol.request(i)
            index, payload = await self._protocol.receive()
            if index == i:
                data.append(payload)
        info = b"".join(data)
        if hashlib.sha1(info).digest() != self._tracker_params.info_hash:
            raise ConnectionError("Peer sent invalid data.")
        self.parent.done(info)

    async def _on_stop(self):
        if self._protocol is None:
            return
        self._protocol.close()
        await self._protocol.wait_closed()
