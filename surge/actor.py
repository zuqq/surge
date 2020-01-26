import asyncio
import logging


class Actor:
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
        if self.running:
            return
        self.running = True
        self.parent = parent
        logging.debug("Starting %r.", self)
        for coro in self._coros:
            self._tasks.add(asyncio.create_task(coro))

    async def spawn_child(self, child):
        await child.start(self)
        self.children.add(child)

    def report_crash(self, reporter):
        if reporter in self.children:
            self._crash()

    def _crash(self, reason=None):
        """Mark the actor as crashed and report the crash to its parent."""
        if self.crashed:
            return
        self.crashed = True
        if reason is not None:
            logging.debug("%r crashed with %r.", self, reason)
        if self.parent is None:
            raise SystemExit(f"Unsupervised actor {self} crashed.")
        self.parent.report_crash(self)

    async def stop(self):
        """Stop all of the actor's children and then itself."""
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
        """Main coroutine run by the actor.
        
        If an exception is thrown here, the actor crashes.

        Example:

            async def _main_coro(self):
                await spawn_additional_children()
                await do_work()
        """
        pass

    async def _on_stop(self):
        """Cleanup.
        
        Example:

            async def _on_stop(self):
                await close_open_connections()
                report_unfinished_tasks()
        """
        pass


class Supervisor(Actor):
    def __init__(self):
        super().__init__()

        self._crashed_children = asyncio.Queue()
        self._coros.add(self._monitor_children())

    def report_crash(self, reporter):
        if reporter in self.children:
            self._crashed_children.put_nowait(reporter)

    async def _monitor_children(self):
        while True:
            child = await self._crashed_children.get()
            await child.stop()
            self.children.remove(child)
            await self._on_child_crash(child)

    ### User logic

    async def _on_child_crash(self, child):
        """Replace crashed children as necessary.
        
        Example:

            class Worker(actor.Actor):
                ...


            class Manager(actor.Supervisor):
                def __init__(self, max_workers=5):
                    super().__init__()
                    self._worker_slots = asyncio.Semaphore(max_workers)

                ...

                async def _spawn_workers(self):
                    await self._worker_slots.acquire()
                    self.spawn_child(Worker())

                def _on_child_crash(self, child):
                    if isinstance(child, Worker):
                        self._worker_slots.release()
        """
        pass
