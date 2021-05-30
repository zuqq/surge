import asyncio


class Channel(asyncio.Queue):
    """An `asyncio.Queue` subclass that implements the async iterator protocol.

    Note that this class is only safe to use as a unidirectional channel with
    one producer and one consumer.
    """

    def __init__(self, maxsize=0):
        # Note that an `asyncio.Queue` with `maxsize <= 0` is unbounded, so we
        # are still able to close it by enqueuing a sentinel object.
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

    def close_nowait(self):
        self.put_nowait(self._sentinel)
