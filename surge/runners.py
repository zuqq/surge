import asyncio
import functools
import logging
import signal


def run(loop, actor):
    def on_signal(s):
        logging.critical("%r received", s)
        actor.set_exception(SystemExit(s.value))

    try:
        for s in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, functools.partial(on_signal, s))
    except NotImplementedError:
        pass
    loop.run_until_complete(actor.start())
    try:
        return loop.run_until_complete(actor)
    finally:
        loop.run_until_complete(actor.stop())
