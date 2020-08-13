"""Download files from the BitTorrent network.

Usage:
    __main__.py (-h | --help)
    __main__.py [--folder FOLDER] [--resume] [--log LOG]
                [--peers PEERS] [--requests REQUESTS]
                (--file FILE | --magnet MAGNET)

Options:
    -h, --help          Show this screen.
    --folder FOLDER     Destination folder
    --resume            Resume the download.
    --log LOG           Log file.
    --peers PEERS       Maximal number of peers [default: 50].
    --requests REQUEST  Maximal number of requests [default: 50].
    --file PATH         Torrent file.
    --magnet MAGNET     Magnet link.

"""

from typing import Dict

import asyncio
import logging
import os
import sys

from docopt import docopt

from . import base
from . import magnet
from . import metadata
from . import mex
from . import tracker


def main(args: Dict[str, str]):
    loop = asyncio.get_event_loop()

    max_peers = int(args["--peers"])
    max_requests = int(args["--requests"])

    if log := args["--log"]:
        logging.basicConfig(
            level=logging.INFO,
            filename=log,
            filemode="w",
            format="%(asctime)s,%(msecs)03d %(levelname)s: %(message)s",
            datefmt="%H:%M:%S",
        )
    else:
        logging.disable(logging.CRITICAL)

    if path := args["--file"]:
        print(f"Reading metadata from {path}.")
        with open(path, "rb") as f:
            raw_meta = f.read()
        meta = metadata.Metadata.from_bytes(raw_meta)
        params = tracker.Parameters.from_bytes(raw_meta)
    else:
        # Flush stdout because the next operation may take a while.
        print("Downloading metadata from peers...", end="", flush=True)
        info_hash, announce_list = magnet.parse(args["--magnet"])
        params = tracker.Parameters(info_hash)
        raw_meta = loop.run_until_complete(
            mex.download(announce_list, params, max_peers)
        )
        meta = metadata.Metadata.from_bytes(raw_meta)
        print("Done.")
        path = f"{info_hash.hex()}.torrent"
        print(f"Writing metadata to {path}.")
        with open(path, "wb") as f:
            f.write(raw_meta)

    if folder := args["--folder"]:
        meta.folder = os.path.join(folder, meta.folder)
        print(f"Downloading to {meta.folder}.")

    print("Building the file tree...", end="", flush=True)
    metadata.build_file_tree(meta.folder, meta.files)
    print("Done.")

    missing = set(meta.pieces)

    if args["--resume"]:
        print("Checking for available pieces...", end="", flush=True)
        for piece in metadata.available_pieces(meta.pieces, meta.folder, meta.files):
            missing.remove(piece)
        print("Done.")

    if not missing:
        print("Nothing to do.")
    else:
        loop.run_until_complete(
            base.download(meta, params, missing, max_peers, max_requests)
        )


if __name__ == "__main__":
    try:
        main(docopt(__doc__))
    except KeyboardInterrupt:
        sys.exit(130)
