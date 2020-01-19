import asyncio


def run(torrent):
    # Adapted from aiohttp.
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(torrent.start())
        loop.run_until_complete(torrent.wait_done())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(torrent.stop())
        tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
