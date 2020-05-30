from typing import List

import asyncio
import functools
import hashlib

from . import actor
from . import bencoding
from . import extension_protocol
from . import metadata
from . import metadata_protocol
from . import peer_protocol
from . import peer_queue
from . import protocol
from . import tracker


def parse(message):
    """Return the pair (type, content) for the given message."""
    mt = peer_protocol.message_type(message)
    if mt == peer_protocol.Message.EXTENSION_PROTOCOL:
        payload = message[1:]
        emt = extension_protocol.message_type(payload)
        if emt == extension_protocol.Message.HANDSHAKE:
            d = bencoding.decode(payload[1:])
            return (emt, (d[b"m"][b"ut_metadata"], d[b"metadata_size"]))
        elif emt == extension_protocol.Message.UT_METADATA:
            d, payload = metadata_protocol.parse_message(payload[1:])
            mmt = metadata_protocol.Message(d[b"msg_type"])
            if mmt == metadata_protocol.Message.DATA:
                return (mmt, (d[b"piece"], payload))
            else:
                return (mmt, None)
    return (mt, None)


class Protocol(protocol.Closed):
    def __init__(self, info_hash, peer_id):
        super().__init__(info_hash, peer_id)

        self._parser = parse

        self.metadata_size = asyncio.get_event_loop().create_future()

        self._ut_metadata = None

        self._transition = {
            (protocol.Established, extension_protocol.Message.HANDSHAKE): (
                self._extension_handshake,
                Choked,
            ),
            (Choked, peer_protocol.Message.UNCHOKE): (None, Unchoked),
            (Choked, metadata_protocol.Message.DATA): (
                self._block_data.put_nowait,
                Choked,
            ),
            (Unchoked, peer_protocol.Message.CHOKE): (None, Choked),
            (Unchoked, metadata_protocol.Message.DATA): (
                self._block_data.put_nowait,
                Unchoked,
            ),
        }

    def _extension_handshake(self, payload):
        ut_metadata, metadata_size = payload
        self._ut_metadata = ut_metadata
        self.metadata_size.set_result(metadata_size)

    def connection_made(self, transport):
        self._state = protocol.Open
        self._transport = transport
        self._write(peer_protocol.handshake(self._info_hash, self._peer_id))
        self._write(extension_protocol.handshake())


class Choked(protocol.Established):
    async def request(self, index):
        if self._exc is not None:
            raise self._exc
        waiter = asyncio.get_running_loop().create_future()
        self._waiters[Unchoked].add(waiter)
        self._write(peer_protocol.interested())
        await waiter
        self._write(metadata_protocol.request(index, self._ut_metadata))


class Unchoked(protocol.Established):
    async def request(self, index):
        if self._exc is not None:
            raise self._exc
        self._write(metadata_protocol.request(index, self._ut_metadata))


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
        self._peer_queue = peer_queue.PeerQueue(announce_list, tracker_params)
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
                Protocol, self._tracker_params.info_hash, self._tracker_params.peer_id,
            ),
            self._peer.address,
            self._peer.port,
        )

        # TODO: Validate the peer's handshake.
        _, _, _ = await self._protocol.handshake

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
