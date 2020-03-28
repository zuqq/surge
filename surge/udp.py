import asyncio


class DatagramStream(asyncio.DatagramProtocol):
    def __init__(self):
        self._inbox = asyncio.Queue()
        self._drained = asyncio.Event()
        self._transport = None

    ### Protocol

    def connection_made(self, transport):
        self._drained.set()
        self._transport = transport

    def datagram_received(self, data, addr):
        self._inbox.put_nowait(data)

    def pause_writing(self):
        self._drained.clear()
        super().pause_writing()

    def resume_writing(self):
        self._drained.set()
        super().resume_writing()

    ### Stream interface

    def write(self, message):
        self._transport.sendto(message)

    async def drain(self):
        await self._drained.wait()

    async def read(self):
        return await self._inbox.get()
