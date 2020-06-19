import asyncio
import functools

from . import _peer
from . import _stream


class Closed(_stream.State):
    @staticmethod
    async def establish(stream):
        return await stream.bitfield

    @staticmethod
    async def receive(stream):
        if not stream.queue:
            waiter = asyncio.get_running_loop().create_future()
            stream.add_waiter(waiter, _peer.Block)
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
        await stream.protocol.write(_peer.Request(block))


class Stream(_stream.Stream):
    def __init__(self, info_hash, peer_id, pieces):
        super().__init__(info_hash, peer_id)

        self.available = set()
        self.bitfield = asyncio.get_event_loop().create_future()
        self.add_waiter(self.bitfield, _peer.Bitfield)

        def on_bitfield(message):
            self.available = message.available(pieces)

        def on_have(message):
            self.available.add(message.piece(pieces))

        def on_block(message):
            self.queue.append((message.block(pieces), message.data))

        self._transition = {
            (Closed, _peer.Handshake): (None, Open),
            (Open, _peer.Bitfield): (on_bitfield, Choked),
            (Choked, _peer.Unchoke): (None, Unchoked),
            (Choked, _peer.Have): (on_have, Choked),
            (Choked, _peer.Block): (on_block, Choked),
            (Unchoked, _peer.Choke): (None, Choked),
            (Unchoked, _peer.Have): (on_have, Unchoked),
            (Unchoked, _peer.Block): (on_block, Unchoked),
        }

        self.state = Closed


async def open_stream(info_hash, peer_id, pieces, address, port):
    stream = Stream(info_hash, peer_id, pieces)
    await asyncio.get_running_loop().create_connection(
        functools.partial(_stream.Protocol, stream), address, port
    )
    return stream
