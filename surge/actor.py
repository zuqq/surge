import asyncio
import logging


class Actor:
    def __init__(self, *, is_supervisor=False):
        self._is_supervisor = is_supervisor
        self._parent = None

        self._running = False
        self._crashed = False
        self._children = set()

        self._crashed_children = asyncio.Queue()
        self._main_task = None
        self._monitor_task = None

    async def _run_on_start(self):
        try:
            await self._on_start()
        except Exception as e:
            self._crash(e)

    async def start(self, parent=None):
        """Start the actor.

        Children are started by add_child.
        """
        if self._running:
            return
        self._running = True
        self._parent = parent
        logging.debug("Starting %r.", self)
        self._main_task = asyncio.create_task(self._run_on_start())
        self._monitor_task = asyncio.create_task(self._monitor_children())

    async def spawn_child(self, child):
        await child.start(self)
        self._children.add(child)

    async def _monitor_children(self):
        while True:
            child = await self._crashed_children.get()
            # If self is not a supervisor, it crashes.
            if not self._is_supervisor:
                self._crash()
            # Otherwise it stops the child and executes the _on_crash callback.
            else:
                await child.stop()
                self._children.remove(child)
                await self._on_child_crash(child)

    def report_crash(self, reporter):
        if reporter in self._children:
            self._crashed_children.put_nowait(reporter)

    def _crash(self, reason=None):
        """Mark the actor as crashed and report the crash to its supervisor."""
        if self._crashed:
            return
        self._crashed = True
        if reason is not None:
            logging.debug("%r crashed with %r.", self, reason)
        if self._parent is None:
            raise SystemExit(f"Unsupervised actor {self} crashed.")
        self._parent.report_crash(self)

    async def stop(self):
        """Stop all of the actor's children and then itself."""
        if not self._running:
            return
        self._running = False
        self._crashed = True  # In case the crash happened in a different branch.

        for child in list(self._children):
            await child.stop()

        logging.debug("Stopping %r.", self)
        tasks = (self._main_task, self._monitor_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._on_stop()

    ### User logic

    async def _on_start(self):
        """Main coroutine run by the actor.
        
        If an exception is thrown here, the actor crashes.

        Example:

            async def _on_start(self):
                await spawn_additional_children()
                await do_work()
        """
        pass

    async def _on_child_crash(self, child):
        """Replace crashed children as necessary.
        
        Example:

            class Worker(actor.Actor):
                def __init__(self, supervisor: Supervisor):
                    super().__init__(parent=supervisor)

                ...


            class Supervisor(actor.Actor):
                def __init__(self, max_workers=5):
                    super().__init__(is_supervisor=True)
                    self._worker_slots = asyncio.Semaphore(max_workers)

                ...

                async def _spawn_workers(self):
                    await self._worker_slots.acquire()
                    self.spawn_child(Worker(self))

                def _on_child_crash(self, child):
                    if isinstance(child, Worker):
                        self._worker_slots.release()
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
