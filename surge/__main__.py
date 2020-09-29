"""Download files from the BitTorrent network.

Usage:
    __main__.py (-h | --help)
    __main__.py [--resume] [--log LOG] [--peers PEERS] [--requests REQUESTS]
                (--file FILE | --magnet MAGNET)

Options:
    -h, --help          Show this screen.
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
import secrets
import sys

from docopt import docopt  # type: ignore
import uvloop

from . import _metadata
from . import magnet
from . import mex
from . import protocol


def main(args: Dict[str, str]) -> None:
    uvloop.install()
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

    peer_id = secrets.token_bytes(20)

    if path := args["--file"]:
        with open(path, "rb") as f:
            raw_metadata = f.read()
        metadata = _metadata.Metadata.from_bytes(raw_metadata)
    else:
        # Flush stdout because the next operation may take a while.
        print("Downloading .torrent file from peers...", end="", flush=True)
        info_hash, announce_list = magnet.parse(args["--magnet"])
        raw_metadata = loop.run_until_complete(
            mex.download(info_hash, announce_list, peer_id, max_peers)
        )
        metadata = _metadata.Metadata.from_bytes(raw_metadata)
        print("Done.")
        path = f"{info_hash.hex()}.torrent"
        print(f"Saving .torrent file to {path}.")
        with open(path, "wb") as f:
            f.write(raw_metadata)

    missing = set(metadata.pieces)

    if args["--resume"]:
        print("Checking for available pieces...", end="", flush=True)
        for piece in _metadata.available(metadata.pieces, metadata.files):
            missing.remove(piece)
        print("Done.")

    if not missing:
        print("Nothing to do.")
    else:
        loop.run_until_complete(
            protocol.download(metadata, peer_id, missing, max_peers, max_requests)
        )


if __name__ == "__main__":
    try:
        main(docopt(__doc__))
    except KeyboardInterrupt:
        sys.exit(130)
