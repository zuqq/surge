import argparse
import asyncio
import logging
import os

import metadata
import torrent


def download_torrent(metainfo):
    # Adapted from aiohttp.
    loop = asyncio.get_event_loop()
    # Note that the instantiation already creates an event loop. If this is
    # not desired, simply move instantiations of asyncio.Queue() etc. into
    # the start() method.
    t = torrent.Torrent(metainfo)
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(t.start())
        loop.run_until_complete(t.wait_done())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(t.stop())
        tasks = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in tasks:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download files from the BitTorrent network."
    )
    parser.add_argument("file", help="path to the torrent file")
    parser.add_argument("--debug", help="enable logging", action="store_true")
    parser.add_argument("--resume", help="resume download", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()

    if args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            filename="debug.log",
            filemode="w",
            format="%(asctime)s,%(msecs)03d %(levelname)s: %(message)s",
            datefmt="%H:%M:%S",
        )

    with open(args.file, "rb") as f:
        metainfo = metadata.Metainfo.from_bytes(f.read())
    metainfo.folder = os.path.join("download", metainfo.folder)
    print(f"Downloading {args.file} to {metainfo.folder}.")

    if args.resume:
        print(f"Checking for existing pieces.")
        metainfo.missing_pieces = metadata.missing_pieces(
            metainfo.pieces, metainfo.files, metainfo.folder
        )
        print(f"Missing {len(metainfo.missing_pieces)}/{len(metainfo.pieces)} pieces.")

    download_torrent(metainfo)

    print("Exiting.")


if __name__ == "__main__":
    main()
