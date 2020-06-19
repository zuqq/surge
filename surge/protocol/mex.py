import asyncio
import functools

from . import _extension
from . import _metadata
from . import _peer
from . import _stream
from . import base


class Closed(_stream.State):
    @staticmethod
    async def establish(stream):
        await stream.protocol.write(_peer.ExtensionProtocol(_extension.Handshake()))
        await stream.handshake
        return stream.metadata_size

    @staticmethod
    async def receive(stream):
        if not stream.queue:
            waiter = asyncio.get_running_loop().create_future()
            stream.add_waiter(waiter, _metadata.Data)
            await waiter
        return stream.queue.popleft()


class Open(Closed):
    pass


class Choked(Closed):
    @staticmethod
    async def request(stream, block):
        waiter = asyncio.get_running_loop().create_future()
        stream.add_waiter(waiter, _peer.Unchoke)
        await stream.protocol.write(_peer.Interested())
        await waiter
        await Unchoked.request(stream, block)


class Unchoked(Closed):
    @staticmethod
    async def request(stream, block):
        await stream.protocol.write(
            _peer.ExtensionProtocol(
                _extension.Metadata(_metadata.Request(block), stream.ut_metadata)
            )
        )


class Stream(_stream.Stream):
    def __init__(self, info_hash, peer_id):
        super().__init__(info_hash, peer_id)

        self.handshake = asyncio.get_event_loop().create_future()
        self.add_waiter(self.handshake, _extension.Handshake)
        self.ut_metadata = None
        self.metadata_size = None

        def on_handshake(message):
            self.ut_metadata = message.ut_metadata
            self.metadata_size = message.metadata_size

        def on_data(message):
            self.queue.append((message.index, message.data))

        self._transition = {
            (Closed, _peer.Handshake): (None, Open),
            (Open, _extension.Handshake): (on_handshake, Choked),
            (Choked, _peer.Unchoke): (None, Unchoked),
            (Choked, _metadata.Data): (on_data, Choked),
            (Unchoked, _peer.Choke): (None, Choked),
            (Unchoked, _metadata.Data): (on_data, Unchoked),
        }

        self.state = Closed


async def open_stream(info_hash, peer_id, address, port):
    stream = Stream(info_hash, peer_id)
    await asyncio.get_running_loop().create_connection(
        functools.partial(_stream.Protocol, stream), address, port
    )
    return stream
