import asyncio


class DatagramStream(asyncio.DatagramProtocol):
    def __init__(self):
        self._transport = None
        self._exception = None
        self._closed = asyncio.Event()
        self._drained = asyncio.Event()
        self._inbox = asyncio.Queue()

    ### asyncio.BaseProtocol

    def connection_made(self, transport):
        self._drained.set()
        self._transport = transport

    def connection_lost(self, exc):
        self._exception = self._exception or exc
        self._closed.set()

    def pause_writing(self):
        self._drained.clear()

    def resume_writing(self):
        self._drained.set()

    ### asyncio.DatagramProtocol

    def datagram_received(self, data, addr):
        self._inbox.put_nowait((data, addr))

    def error_received(self, exc):
        self._exception = self._exception or exc

    ### Interface

    def send(self, data):
        self._transport.sendto(data, addr=None)

    async def drain(self):
        if self._exception is not None:
            raise self._exception
        await self._drained.wait()

    async def recv(self):
        if self._exception is not None:
            raise self._exception
        data, _ = await self._inbox.get()
        return data

    def close(self):
        if self._transport is not None:
            self._transport.close()

    async def wait_closed(self):
        await self._closed.wait()
