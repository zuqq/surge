import asyncio
import functools
import logging
import signal


def run(actor):
    def on_signal(s):
        logging.critical("%r received", s)
        actor.set_result(None)

    loop = asyncio.get_event_loop()
    try:
        for s in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(s, functools.partial(on_signal, s))
    except NotImplementedError:
        pass
    loop.run_until_complete(actor.start())
    result = loop.run_until_complete(actor)
    loop.run_until_complete(actor.stop())
    return result
