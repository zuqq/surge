import asyncio


class Channel(asyncio.Queue):
    def __init__(self, maxsize: int = 0):
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
