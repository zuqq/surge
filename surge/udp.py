import asyncio


class DatagramStream(asyncio.DatagramProtocol):
    def __init__(self):
        self._transport = None
        self._exception = None
        self._closed = asyncio.Event()
        self._drained = asyncio.Event()
        self._inbox = asyncio.Queue()

    ### Protocol

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

    def datagram_received(self, data, addr):
        self._inbox.put_nowait(data)

    def error_received(self, exc):
        self._exception = self._exception or exc

    ### Stream interface

    def write(self, message):
        self._transport.sendto(message)

    async def drain(self):
        if self._exception is not None:
            raise self._exception
        await self._drained.wait()

    async def read(self):
        if self._exception is not None:
            raise self._exception
        return await self._inbox.get()

    def close(self):
        if self._transport is not None:
            self._transport.close()

    async def wait_closed(self):
        await self._closed.wait()
