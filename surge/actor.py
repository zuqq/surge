from __future__ import annotations
from typing import Awaitable, Callable, Optional, Set

import asyncio
import logging


class FutureMixin:
    def __init__(self):
        self._future = asyncio.get_event_loop().create_future()

    def __await__(self):
        return iter(self._future)

    def set_result(self, result):
        if not self._future.done():
            self._future.set_result(result)

    def set_exception(self, exception):
        if not self._future.done():
            self._future.set_exception(exception)

    def done(self):
        return self._future.done()

    def result(self):
        return self._future.result()

    def exception(self):
        return self._future.exception()


class Actor(FutureMixin):
    """Actor base class.

    `Actor`s form a directed graph whose structure is stored in the attributes
    `parent` and `children`; acyclicity of this graph is not enforced.

    The principal purpose of an `Actor` is to run the coroutine `_main`.
    In doing so, it may spawn children and pass messages to its parent and
    children. Messages are to be implemented as methods of the receiving class.

    `Exception`s in `_main` cause the `Actor` to crash; if it has a parent,
    the crash bubbles up. An ordinary `Actor` that receives a crash report from
    one of its children crashes itself. Instances of the subclass `Supervisor`
    can handle crash reports gracefully instead.

    `Actor`s are controlled via the methods `start` and `stop`; stopping an
    `Actor` also stops all of its children.
    """

    def __init__(self):
        super().__init__()

        self.parent: Optional[Actor] = None
        self.children: Set[Actor] = set()

        self._running = False
        self._crashed = False
        self._coros: Set[Callable[[], Awaitable]] = {self._main}
        self._tasks: Set[asyncio.Task] = set()
        self._runner: Optional[asyncio.Task] = None

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
        self.set_exception(reason)
        if self.parent is not None:
            self.parent.report_crash(self)

        logging.warning("%r crashed with %r", self, reason)

    async def _run(self):
        for coro in self._coros:
            self._tasks.add(asyncio.create_task(coro()))
        try:
            await asyncio.gather(*self._tasks)
        except Exception as e:
            self._crash(e)

    ### Overridable methods

    async def _main(self):
        pass

    async def _on_stop(self):
        pass

    def report_crash(self, reporter: Actor):
        if reporter in self.children:
            self._crash(RuntimeError(f"Uncaught crash: {reporter}"))

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
            raise RuntimeError("Calling 'spawn_child' on stopped actor.")
        child.parent = self
        self.children.add(child)
        await child.start()

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
        self.set_result(None)

        logging.debug("%r stopped", self)


class Supervisor(Actor):
    """Supervisor base class.

    `Supervisor`s are `Actor`s that supervise their children. By default this
    amounts to shutting down any crashed children and discarding them; more
    complex behavior needs to be implemented by the user.
    """

    def __init__(self):
        super().__init__()

        self._crashed_children = asyncio.Queue()
        self._coros.add(self._supervise)

    async def _supervise(self):
        while True:
            child = await self._crashed_children.get()
            await child.stop()
            _ = child.exception()
            self.children.remove(child)
            await self._on_child_crash(child)

    def report_crash(self, reporter: Actor):
        if reporter in self.children:
            self._crashed_children.put_nowait(reporter)

    ### Overridable methods

    async def _on_child_crash(self, child: Actor):
        raise RuntimeError(f"Uncaught crash: {child}")
