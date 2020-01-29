import asyncio
import logging


class Actor:
    """Base class for all actors.

    Actors are controlled from the outside via the methods start() and stop().
    They can have children, which are automatically stopped when their parent
    is stopped.

    The principal purpose of an actor is to run its main coroutine. Exceptions
    that occur in this coroutine cause the actor to crash; if it has a parent,
    the crash is reported. An ordinary actor that receives a crash report from
    one of its children crashes itself; instances of the subclass Supervisor can
    handle crash reports gracefully instead of crashing themselves.
    """
    def __init__(self):
        self.parent = None

        self.running = False
        self.crashed = False

        self.children = set()

        self._coros = {self._run_main_coro()}
        self._tasks = set()

    async def _run_main_coro(self):
        try:
            await self._main_coro()
        except Exception as e:
            self._crash(e)

    async def start(self, parent=None):
        """Start the actor."""
        if self.running:
            return
        self.running = True
        self.parent = parent
        logging.debug("Starting %r.", self)
        for coro in self._coros:
            self._tasks.add(asyncio.create_task(coro))

    async def spawn_child(self, child):
        """Start the given actor and add it as a child."""
        await child.start(self)
        self.children.add(child)

    def _crash(self, reason=None):
        if self.crashed:
            return
        self.crashed = True
        if reason is not None:
            logging.debug("%r crashed with %r.", self, reason)
        if self.parent is None:
            raise SystemExit(f"Unsupervised actor {self} crashed.")
        self.parent.report_crash(self)

    def report_crash(self, reporter):
        """Report that a child crashed, which crashes the actor itself."""
        if reporter in self.children:
            self._crash()

    async def stop(self):
        """Stop all of the actor's children and then the actor itself."""
        if not self.running:
            return
        self.running = False
        self.crashed = True  # In case the crash happened in a different branch.

        for child in list(self.children):
            await child.stop()

        logging.debug("Stopping %r.", self)
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._on_stop()

    ### User logic

    async def _main_coro(self):
        pass

    async def _on_stop(self):
        pass


class Supervisor(Actor):
    """Base class for all supervisors.

    Supervisors are actors that supervise their children. By default this
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

    def report_crash(self, reporter):
        """Report that a child crashed."""
        if reporter in self.children:
            self._crashed_children.put_nowait(reporter)

    ### User logic

    async def _on_child_crash(self, child):
        pass
