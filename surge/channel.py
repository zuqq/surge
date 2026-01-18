import asyncio
from typing import Any, Self


class Channel[T](asyncio.Queue[T]):
    """An `asyncio.Queue` subclass that implements the async iterator protocol.

    Note that this class is only safe to use as a unidirectional channel with
    one producer and one consumer.
    """

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._sentinel: Any = object()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> T:
        if (item := await self.get()) is self._sentinel:
            raise StopAsyncIteration
        return item

    async def close(self) -> None:
        await self.put(self._sentinel)

    def close_nowait(self) -> None:
        self.put_nowait(self._sentinel)
