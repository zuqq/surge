import asyncio

from . import _extension
from . import _metadata
from . import _peer
from . import base


class Closed(base.Closed):
    async def establish(self):
        exc = self._exception
        if exc is not None:
            raise exc
        await self.protocol.write(_peer.Handshake(self._info_hash, self._peer_id))
        await self.protocol.write(_peer.ExtensionProtocol(_extension.Handshake()))
        await self._handshake
        return self._metadata_size

    async def receive(self):
        exc = self._exception
        if exc is not None:
            raise exc
        if not self._queue:
            waiter = asyncio.get_running_loop().create_future()
            self._waiters[_metadata.Data].add(waiter)
            await waiter
        return self._queue.popleft()


class Open(Closed):
    pass


class Choked(Closed):
    async def request(self, index):
        waiter = asyncio.get_running_loop().create_future()
        self._waiters[_peer.Unchoke].add(waiter)
        await self.protocol.write(_peer.Interested())
        await waiter
        await self.protocol.write(
            _peer.ExtensionProtocol(
                _extension.Metadata(_metadata.Request(index), self._ut_metadata)
            )
        )


class Unchoked(Closed):
    async def request(self, index):
        await self.protocol.write(
            _peer.ExtensionProtocol(
                _extension.Metadata(_metadata.Request(index), self._ut_metadata)
            )
        )


class Stream(Closed):
    def __init__(self, info_hash, peer_id):
        super().__init__(info_hash, peer_id)

        self._info_hash = info_hash
        self._peer_id = peer_id

        self._handshake = asyncio.get_event_loop().create_future()
        self._waiters[_extension.Handshake].add(self._handshake)
        self._ut_metadata = None
        self._metadata_size = None

        def on_handshake(message):
            self._ut_metadata = message.ut_metadata
            self._metadata_size = message.metadata_size

        def on_data(message):
            self._queue.append((message.index, message.data))

        self._transition = {
            (Closed, _peer.Handshake): (None, Open),
            (Open, _extension.Handshake): (on_handshake, Choked),
            (Choked, _peer.Unchoke): (None, Unchoked),
            (Choked, _metadata.Data): (on_data, Choked),
            (Unchoked, _peer.Choke): (None, Choked),
            (Unchoked, _metadata.Data): (on_data, Unchoked),
        }

        self.state = Closed
