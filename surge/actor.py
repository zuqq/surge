from __future__ import annotations
from typing import Awaitable, Optional, Set

import asyncio
import logging


class UncaughtCrash(Exception):
    pass


class InvalidState(Exception):
    pass


class Actor:
    """Actor base class.

    The principal purpose of an `Actor` is to run the coroutine `_main`.
    In doing so, it may spawn children and pass messages to its parent and
    children. Messages are to be implemented as methods of the receiving class.

    Raising an `Exception` in `_main` causes the `Actor` to crash. If it has a
    parent, the crash bubbles up. By default, an `Actor` that receives a crash
    report from one of its children crashes itself. Overriding `_on_child_crash`
    changes this behavior.
    """

    def __init__(self):
        self.parent: Optional[Actor] = None
        self.children: Set[Actor] = set()

        self._running = False
        self._crashed = False

        self.result = asyncio.get_event_loop().create_future()
        self._tasks: Set[asyncio.Task] = set()
        self._runner: Optional[asyncio.Task] = None

        self._crashed_children = asyncio.Queue()

    @property
    def running(self):
        return self._running

    @property
    def crashed(self):
        return self._crashed

    def _crash(self, reason: Exception):
        if self._crashed or not self._running:
            return
        self._crashed = True
        logging.warning("%r crashed with %r", self, reason)

        if not self.result.done():
            self.result.set_exception(reason)
        if self.parent is not None:
            self.parent.report_crash(self)

    async def _run(self):
        for coro in (self._main(), self._supervisor()):
            self._tasks.add(asyncio.create_task(coro))
        try:
            await asyncio.gather(*self._tasks)
        except Exception as e:
            self._crash(e)

    async def _supervisor(self):
        while True:
            child = await self._crashed_children.get()
            await child.stop()
            _ = child.result.exception()
            self.children.remove(child)
            await self._on_child_crash(child)

    ### Overridable methods

    async def _main(self):
        pass

    async def _on_child_crash(self, child: Actor):
        raise UncaughtCrash(child)

    async def _on_stop(self):
        pass

    ### Interface

    async def start(self):
        if self._running:
            return
        self._running = True
        self._runner = asyncio.create_task(self._run())
        logging.debug("%r started", self)

    async def spawn_child(self, child: Actor):
        """Start `child` and add it to `self`'s children."""
        if not self._running:
            raise InvalidState("Calling 'spawn_child' on stopped actor.")
        child.parent = self
        self.children.add(child)
        await child.start()

    def report_crash(self, reporter: Actor):
        if reporter in self.children:
            self._crashed_children.put_nowait(reporter)

    async def stop(self):
        """First stop `self`, then all of its children."""
        if not self._running:
            return
        self._running = False
        self._runner.cancel()
        try:
            await self._runner
        except asyncio.CancelledError:
            pass
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for child in self.children:
            await child.stop()
        await self._on_stop()
        logging.debug("%r stopped", self)
