from __future__ import annotations
from typing import Optional, Set

import asyncio
import logging


class _Future:
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


class Actor(_Future):
    def __init__(self, parent: Optional[Actor] = None):
        super().__init__()

        self.parent = parent
        self.children: Set[Actor] = set()

        self._running = False
        self.tasks: Set[asyncio.Task] = set()

        self._crashed = False
        self._crashed_children = asyncio.Queue()  # type: ignore

    def _crash(self, reason: Exception):
        if self._crashed or not self._running:
            return
        logging.warning("%r crashed with %r", self, reason)
        self._crashed = True
        self.set_exception(reason)
        if self.parent is not None:
            self.parent.report_crash(self)

    async def _run(self):
        try:
            await self._main()
        except Exception as e:
            self._crash(e)

    async def _supervise(self):
        while True:
            child = await self._crashed_children.get()
            await child.stop()
            self.children.remove(child)
            await self._on_child_crash(child)

    # Overridable methods

    async def _main(self):
        pass

    async def _on_child_crash(self, child: Actor):
        self._crash(RuntimeError(f"Uncaught crash: {child}"))

    # Interface

    @property
    def running(self):
        return self._running

    @property
    def crashed(self):
        return self._crashed

    async def start(self):
        if self._running:
            return
        logging.debug("%r starting", self)
        self._running = True
        self.tasks.add(asyncio.create_task(self._run()))
        self.tasks.add(asyncio.create_task(self._supervise()))
        for child in self.children:
            await child.start()

    def report_crash(self, reporter: Actor):
        self._crashed_children.put_nowait(reporter)

    async def stop(self):
        """First stop `self`, then all of its children."""
        if not self._running:
            return
        logging.debug("%r stopping", self)
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self._running = False
        self.set_result(None)
        self.exception()
        for child in self.children:
            await child.stop()
