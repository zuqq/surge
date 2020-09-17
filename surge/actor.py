from __future__ import annotations
from typing import Coroutine, Optional, Set

import asyncio
import contextlib
import logging


class Actor:
    """Sets of coroutines with support for message passing and error handling.

    `Actor`s form a directed graph whose structure is stored in the attributes
    `parent` and `children`.

    The principal purpose of an `Actor` is to run the coroutines contained in
    `_coros`. In doing so, it may spawn children and pass messages to its parent
    and children. Messages are to be implemented as methods of the receiving
    class.

    `Actor`s are controlled via the methods `start` and `stop`; starting or
    stopping an `Actor` does the same to all of its children.

    If one of its coroutines raises an `Exception`, the `Actor` notifies its
    parent; the parent then shuts down the affected `Actor`. Restart strategies
    can be added to subclasses by overriding `_supervise`.
    """

    def __init__(self, parent: Optional[Actor] = None):
        self.parent = parent
        self.children: Set[Actor] = set()
        self.running = False

        self._coros: Set[Coroutine[None, None, None]] = {self._supervise()}
        self._runner: Optional[asyncio.Task] = None
        self._tasks: Set[asyncio.Task] = set()

        # This queue is unbounded because the actor is supposed to control the
        # number of children it spawns (and thereby the number of concurrent
        # crashes that can occur).
        self._crashes = asyncio.Queue()  # type: ignore

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()
        return False

    async def _run(self):
        self._tasks = {asyncio.create_task(coro) for coro in self._coros}
        try:
            await asyncio.gather(*self._tasks)
        # It's okay to catch `Exception` here because `asyncio.CancelledError`
        # derives directly from `BaseException` in Python 3.8.
        except Exception as exc:
            if self.running:
                logging.warning("%r crashed with %r", self, exc)
                if self.parent is not None:
                    self.parent.report_crash(self)

    def _on_child_crash(self, child: Actor):
        raise RuntimeError(f"Uncaught crash: {child}")

    async def _supervise(self):
        while True:
            child = await self._crashes.get()
            await child.stop()
            self.children.remove(child)
            self._on_child_crash(child)

    async def start(self) -> None:
        """First start `self`, then all of its children."""
        # This method is async because it requires a running event loop.
        if self.running:
            return
        logging.info("%r starting", self)
        self.running = True
        self._runner = asyncio.create_task(self._run())
        for child in self.children:
            await child.start()

    async def spawn_child(self, child: Actor) -> None:
        self.children.add(child)
        await child.start()

    def report_crash(self, child: Actor) -> None:
        """Report to `self` that `child` crashed."""
        self._crashes.put_nowait(child)

    async def stop(self) -> None:
        """First stop `self`, then all of its children."""
        if not self.running:
            return
        logging.info("%r stopping", self)
        self.running = False
        if self._runner is not None:
            self._runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._runner
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for child in self.children:
            await child.stop()
