import asyncio
import logging


def run(download):
    # Adapted from aiohttp.
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(download.start())
        return loop.run_until_complete(download.wait_done())
    except KeyboardInterrupt:
        logging.debug("Received KeyboardInterrupt.")
    finally:
        loop.run_until_complete(download.stop())
        tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
