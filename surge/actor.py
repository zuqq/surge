"""Actor base class.

The actor model is a concurrency paradigm that empasizes the use of message
passing instead of shared state. Apart from sending and receiving messages,
actors can also spawn other actors. The resulting tree-like structure gives us
a convenient way of dealing with exceptions in an actor's thread of execution:
it simply messages its parent, who can then decide to restart the child or
replace it entirely. This makes the model especially suitable for concurrent
communication over computer networks where connections are frequently expected
to be dropped.

As to the concrete implementation, actors are objects that derive from the
`Actor` class. Messages are passed by calling methods of the receiving object,
both synchronous and asynchronous in nature. An actor's thread of execution
consists of a set of coroutines; exceptions in any of the coroutines are caught
by a wrapper task. To implement a restart strategy, override `_supervise`.
"""

from __future__ import annotations
from typing import Coroutine, Optional, Set

import asyncio
import contextlib
import weakref


class Actor:
    """Actor base class.

    `Actor`s form a directed graph whose structure is stored in the attributes
    `parent` and `children`.

    The principal purpose of an `Actor` is to run the coroutines contained in
    `_coros`. In doing so, it may spawn children and pass messages to its parent
    and children. Messages are to be implemented as methods of the receiving
    class.

    The lifetime of an `Actor` is controlled via the methods `start` and `stop`;
    starting orstopping an `Actor` does the same to all of its children.

    If any of its coroutines raises an `Exception`, the `Actor` messages its
    `parent`, who then shuts down the affected `Actor`. Restart strategies can
    be added by overriding `_supervise`.
    """

    def __init__(self, parent: Optional[Actor] = None):
        self._parent = None if parent is None else weakref.ref(parent)
        self.children: Set[Actor] = set()

        self._coros: Set[Coroutine[None, None, None]] = {self._supervise()}
        self._runner: Optional[asyncio.Task] = None
        self._running = False
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
        except Exception:
            if self._running and self.parent is not None:
                self.parent.report_crash(self)

    def _on_child_crash(self, child: Actor):
        raise RuntimeError(f"Uncaught crash: {child}")

    async def _supervise(self):
        while True:
            child = await self._crashes.get()
            await child.stop()
            self.children.remove(child)
            self._on_child_crash(child)

    @property
    def parent(self):
        if self._parent is None:
            return None
        return self._parent()

    async def start(self) -> None:
        """First start `self`, then all of its children."""
        # This method is async because it requires a running event loop.
        if self._running:
            return
        self._running = True
        self._runner = asyncio.create_task(self._run())
        for child in self.children:
            await child.start()

    async def spawn_child(self, child: Actor) -> None:
        """Add `child` to `self.children`, then start it."""
        self.children.add(child)
        await child.start()

    def report_crash(self, child: Actor) -> None:
        """Report that `child` crashed."""
        self._crashes.put_nowait(child)

    async def stop(self) -> None:
        """First stop `self`, then all of its children."""
        if not self._running:
            return
        self._running = False
        if self._runner is not None:
            self._runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._runner
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for child in self.children:
            await child.stop()
