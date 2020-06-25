from __future__ import annotations
from typing import Coroutine, Optional, Set

import asyncio
import logging


class Actor:
    def __init__(self, parent: Optional[Actor] = None):
        self.parent = parent
        self.children: Set[Actor] = set()
        self.running = False

        self._coros: Set[Coroutine[None, None, None]] = {self._supervise()}
        self._runner: Optional[asyncio.Task] = None
        self._tasks: Set[asyncio.Task] = set()

        # This queue is unbounded because the actor is supposed to control the
        # number of children it spawns (and thereby the number of concurrent
        # crashes that can occur).
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
        except Exception as e:
            logging.warning("%r crashed with %r", self, e)
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

    async def start(self):
        # This method is async because it requires a running event loop.
        if self.running:
            return
        logging.info("%r starting", self)
        self.running = True
        self._runner = asyncio.create_task(self._run())
        for child in self.children:
            await child.start()

    async def spawn_child(self, child: Actor):
        self.children.add(child)
        await child.start()

    def report_crash(self, child: Actor):
        self._crashes.put_nowait(child)

    async def stop(self):
        """First stop `self`, then all of its children."""
        if not self.running:
            return
        logging.info("%r stopping", self)
        self.running = False
        if self._runner is not None:
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
