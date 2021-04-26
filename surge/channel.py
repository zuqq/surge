import asyncio


class Channel(asyncio.Queue):
    """A subclass of `asyncio.Queue` that implements the async iterator protocol.

    Note that this class is only safe to use as a unidirectional channel with
    one producer and one consumer.
    """

    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self._sentinel = object()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if (item := await self.get()) is self._sentinel:
            raise StopAsyncIteration
        return item

    async def close(self):
        await self.put(self._sentinel)
