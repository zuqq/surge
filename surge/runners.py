import asyncio
import functools
import logging
import signal


def handler(sig):
    logging.critical("%r", sig)
    raise SystemExit(sig.value)


def run(actor):
    loop = asyncio.get_event_loop()
    try:
        for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, functools.partial(handler, sig))
    except NotImplementedError:
        pass

    loop.run_until_complete(actor.start())
    result = loop.run_until_complete(actor.result)

    loop.run_until_complete(actor.stop())
    tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
    for task in tasks:
        task.cancel()
    loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
    loop.run_until_complete(loop.shutdown_asyncgens())

    return result
