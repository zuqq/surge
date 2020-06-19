import asyncio
import functools
import logging
import signal
import sys


def run(actor):
    loop = asyncio.get_event_loop()

    if sys.platform != "win32":  # Same check as in `asyncio.__init__`.
        def on_signal(s):
            logging.critical("%r received", s)
            actor.set_exception(SystemExit(s.value))
        for s in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, functools.partial(on_signal, s))

    loop.run_until_complete(actor.start())
    try:
        return loop.run_until_complete(actor)
    finally:
        loop.run_until_complete(actor.stop())
