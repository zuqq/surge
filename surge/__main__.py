"""Download files from the BitTorrent network.

Usage:
    __main__.py (-h | --help)
    __main__.py [--folder FOLDER] [--resume] [--peers PEERS] [--log LOG]
                (--file FILE | --magnet MAGNET)

Options:
    -h, --help          Show this screen.
    --folder FOLDER     Destination folder
    --resume            Resume the download.
    --peers PEERS       Maximal number of peers [default: 50].
    --log LOG           Log file.
    --file FILE         Torrent file.
    --magnet MAGNET     Magnet link.

"""

from typing import Dict

import argparse
import logging
import os

from docopt import docopt

from . import base
from . import bencoding
from . import magnet
from . import metadata
from . import mex
from . import runners
from . import tracker


def main(args: Dict[str, str]):
    if args["--log"]:
        logging.basicConfig(
            level=logging.DEBUG,
            filename=args["--log"],
            filemode="w",
            format="%(asctime)s,%(msecs)03d %(levelname)s: %(message)s",
            datefmt="%H:%M:%S",
        )
    else:
        logging.disable(logging.CRITICAL)

    max_peers = int(args["--peers"])

    if args["--file"]:
        path = args["--file"]
        print(f"Reading metadata from {path}.")
        with open(path, "rb") as f:
            raw_meta = f.read()

        meta = metadata.Metadata.from_bytes(raw_meta)
        params = tracker.Parameters.from_bytes(raw_meta)
    elif args["--magnet"]:
        print("Getting metadata file from peers...", end="")
        info_hash, announce_list = magnet.parse(args["--magnet"])
        params = tracker.Parameters(info_hash)
        raw_info = runners.run(mex.Download(params, announce_list, max_peers))
        print("Done.")

        raw_meta = metadata.from_info(announce_list, raw_info)
        meta = metadata.Metadata.from_bytes(raw_meta)

        path = f"{info_hash.hex()}.torrent"
        print(f"Writing metadata to {path}.")
        with open(path, "wb") as f:
            f.write(raw_meta)

    print("Building the file tree...", end="")
    metadata.build_file_tree(meta.folder, meta.files)
    print("Done.")

    if args["--folder"]:
        meta.folder = os.path.join(args["--folder"], meta.folder)
        print(f"Downloading to {meta.folder}.")

    outstanding = set(meta.pieces)

    if args["--resume"]:
        print("Checking for available pieces...", end="")
        for piece in metadata.available_pieces(meta.pieces, meta.files, meta.folder):
            outstanding.remove(piece)
        print("Done.")

    if not outstanding:
        print("Nothing to do.")
    else:
        runners.run(base.Download(meta, params, outstanding, max_peers))


if __name__ == "__main__":
    main(docopt(__doc__))
