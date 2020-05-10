from __future__ import annotations
from typing import Awaitable, Optional, Set

import asyncio
import logging


class Actor:
    """Actor base class.

    `Actor`s form a directed graph whose structure is stored in the attributes
    `parent` and `children`; acyclicity of this graph is not enforced.

    The principal purpose of an `Actor` is to run the coroutine `_main_coro`.
    In doing so, it may spawn children and pass messages to its parent and
    children. Messages are to be implemented as methods of the receiving class.

    `Exception`s in `_main_coro` cause the `Actor` to crash; if it has a parent,
    the crash bubbles up. An ordinary `Actor` that receives a crash report from
    one of its children crashes itself. Instances of the subclass `Supervisor`
    can handle crash reports gracefully instead.

    `Actor`s are controlled via the methods `start` and `stop`; stopping an
    `Actor` also stops all of its children.
    """

    def __init__(self):
        self.parent: Optional[Actor] = None

        self.running = False
        self.crashed = False

        self.children: Set[Actor] = set()

        self._coros: Set[Awaitable] = {self._run_main_coro()}
        self._tasks: Set[asyncio.Task] = set()

    async def _run_main_coro(self):
        try:
            await self._main_coro()
        except Exception as e:
            self._crash(e)

    async def start(self, parent: Optional[Actor] = None):
        """Start `self` and set its parent to `parent`."""
        if self.running:
            return
        self.running = True

        self.parent = parent

        for coro in self._coros:
            self._tasks.add(asyncio.create_task(coro))

        logging.debug("%r started", self)

    async def spawn_child(self, child: Actor):
        """Start `child` and add it to `self`'s children."""
        await child.start(self)
        self.children.add(child)

    def _crash(self, reason: Optional[Exception] = None):
        if self.crashed:
            return
        self.crashed = True

        if reason is not None:
            logging.warning("%r crashed with %r", self, reason)

        if not self.running:
            return
        if self.parent is None:
            raise SystemExit(f"Unsupervised actor {repr(self)} crashed.")
        self.parent.report_crash(self)

    def report_crash(self, reporter: Actor):
        """Signal that `reporter` crashed, which crashes `self`.

        If `self` has no parent, `SystemExit` is raised.
        """
        if reporter in self.children:
            self._crash()

    async def stop(self):
        """First stop `self`, then all of its children."""
        if not self.running:
            return
        self.running = False

        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

        for child in self.children:
            await child.stop()

        await self._on_stop()

        logging.debug("%r stopped", self)

    ### User logic

    async def _main_coro(self):
        pass

    async def _on_stop(self):
        pass


class Supervisor(Actor):
    """Supervisor base class.

    `Supervisor`s are `Actor`s that supervise their children. By default this
    amounts to shutting down any crashed children and discarding them; more
    complex behavior needs to be implemented by the user.
    """

    def __init__(self):
        super().__init__()

        self._crashed_children = asyncio.Queue()
        self._coros.add(self._monitor_children())

    async def _monitor_children(self):
        while True:
            child = await self._crashed_children.get()
            await child.stop()
            self.children.remove(child)
            await self._on_child_crash(child)

    def report_crash(self, reporter: Actor):
        """Signal that `reporter` crashed."""
        if reporter in self.children:
            self._crashed_children.put_nowait(reporter)

    ### User logic

    async def _on_child_crash(self, child: Actor):
        pass
