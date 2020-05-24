from typing import List

import asyncio
import hashlib
import struct

from . import actor
from . import bencoding
from . import extension_protocol
from . import metadata_protocol
from . import peer_protocol
from . import peer_queue
from . import tracker


class Download(actor.Supervisor):
    def __init__(
            self,
            announce_list: List[str],
            tracker_params: tracker.Parameters,
            *,
            max_peers: int = 50,
        ):
        super().__init__()

        self._announce_list = announce_list
        self._tracker_params = tracker_params

        self._peer_queue = None

        self._result = None
        self._done = asyncio.Event()
        self._peer_connection_slots = asyncio.Semaphore(max_peers)

    async def _spawn_peer_connections(self):
        while True:
            await self._peer_connection_slots.acquire()
            peer = await self._peer_queue.get()
            await self.spawn_child(PeerConnection(self, self._tracker_params, peer))

    async def wait_done(self) -> bytes:
        """Return the raw metainfo file once it has been downloaded."""
        await self._done.wait()
        return self._result

    ### Actor implementation

    async def _main_coro(self):
        self._peer_queue = peer_queue.PeerQueue(
            self._announce_list, self._tracker_params
        )
        await self.spawn_child(self._peer_queue)
        await self._spawn_peer_connections()

    async def _on_child_crash(self, child):
        if isinstance(child, PeerConnection):
            self._peer_connection_slots.release()
        else:
            self._crash(RuntimeError(f"Irreplaceable actor {repr(child)} crashed."))

    ### Messages from PeerConnection

    def done(self, raw_metainfo: bytes):
        """Signal that `raw_metainfo` has been received and verified."""
        self._result = raw_metainfo
        self._done.set()


class PeerConnection(actor.Actor):
    def __init__(
            self,
            download: Download,
            tracker_params: tracker.Parameters,
            peer: tracker.Peer,
            *,
            max_requests: int = 10,
        ):
        super().__init__()

        self._download = download
        self._tracker_params = tracker_params

        self._peer = peer

        self._reader = None
        self._writer = None
        self._unchoked = asyncio.Event()

        self._ut_metadata = None
        self._metadata_size = None

        self._outstanding = None
        self._data = {}

        self._timer = {}
        self._slots = asyncio.Semaphore(max_requests)

    async def _read_peer_message(self):
        len_prefix = int.from_bytes(await self._reader.readexactly(4), "big")
        message = await self._reader.readexactly(len_prefix)
        return peer_protocol.message_type(message), message[1:]

    async def _connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._peer.address, self._peer.port
        )

        self._writer.write(
            peer_protocol.handshake(
                self._tracker_params.info_hash, self._tracker_params.peer_id
            )
        )
        await self._writer.drain()

        message = extension_protocol.handshake()
        self._writer.write(message)
        await self._writer.drain()

        _ = await self._reader.readexactly(68)

        while True:
            message_type, payload = await self._read_peer_message()
            if message_type == peer_protocol.Message.BITFIELD:
                continue
            if message_type == peer_protocol.Message.EXTENSION_PROTOCOL:
                break
            raise ConnectionError("Peer sent unexpected message.")

        try:
            _ = extension_protocol.message_type(payload)
        except ValueError:
            raise ConnectionError("Peer sent invalid extension protocol message.")
        d = bencoding.decode(payload[1:])
        self._ut_metadata = d[b"m"][b"ut_metadata"]
        self._metadata_size = d[b"metadata_size"]

    async def _disconnect(self):
        if self._writer is None:
            return
        self._writer.close()
        try:
            await self._writer.wait_closed()
        # https://bugs.python.org/issue38856
        except (BrokenPipeError, ConnectionResetError):
            pass

    async def _unchoke(self):
        if self._unchoked.is_set():
            return
        self._writer.write(peer_protocol.interested())
        await self._writer.drain()
        await self._unchoked.wait()

    async def _timeout(self, *, timeout=10):
        await asyncio.sleep(timeout)
        self._crash(TimeoutError(f"Request timed out."))

    async def _receive(self):
        while True:
            message_type, payload = await self._read_peer_message()
            if message_type == peer_protocol.Message.CHOKE:
                self._unchoked.clear()
            elif message_type == peer_protocol.Message.UNCHOKE:
                self._unchoked.set()
            elif message_type == peer_protocol.Message.EXTENSION_PROTOCOL:
                message_type = extension_protocol.message_type(payload)
                if message_type != extension_protocol.Message.UT_METADATA:
                    raise ConnectionError("Peer sent invalid extension message.")
                message, data = metadata_protocol.parse_message(payload[1:])
                if message[b"msg_type"] == metadata_protocol.Message.DATA.value:
                    index = message[b"piece"]
                    if index not in self._timer:
                        continue
                    self._timer.pop(index).cancel()
                    self._slots.release()
                    self._outstanding -= 1
                    self._data[index] = data
                elif message[b"msg_type"] == metadata_protocol.Message.REJECT.value:
                    raise ConnectionError("Peer sent reject.")
                else:
                    raise ConnectionError("Peer sent invalid metadata message.")
                if not self._outstanding:
                    data = b"".join(self._data[index] for index in sorted(self._data))
                    if hashlib.sha1(data).digest() != self._tracker_params.info_hash:
                        raise ConnectionError("Peer sent invalid data.")
                    self._download.done(data)

    async def _request(self):
        for i in range(self._outstanding):
            await self._unchoke()
            await self._slots.acquire()
            message = metadata_protocol.request(i, self._ut_metadata)
            self._writer.write(message)
            await self._writer.drain()
            self._timer[i] = asyncio.create_task(self._timeout())

    ### Actor implementation

    async def _main_coro(self):
        await self._connect()
        # Integer division that rounds up.
        self._outstanding = (self._metadata_size + 2 ** 14 - 1) // 2 ** 14
        await asyncio.gather(self._receive(), self._request())

    async def _on_stop(self):
        await self._disconnect()
