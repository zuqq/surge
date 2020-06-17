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

import asyncio
import logging
import os

from docopt import docopt

from . import base
from . import magnet
from . import metadata
from . import mex
from . import runners
from . import tracker


async def _print(max_peers, pieces, poll):
    peers = 0
    peers_digits = len(str(max_peers))
    outstanding = pieces
    pieces_digits = len(str(pieces))

    while outstanding:
        peers, outstanding = await poll()
        downloaded = pieces - outstanding
        progress = (
            "Downloading from"
            f" {peers : >{peers_digits}} peers:"
            f" {downloaded : >{pieces_digits}}/{pieces} pieces"
        )
        width, _ = os.get_terminal_size()
        parts = width - len(progress) - 4
        if parts < 10:
            print("\r\x1b[K" + progress, end="")
        else:
            bar = f"[{(parts * downloaded // pieces) * '#' : <{parts}}]"
            print("\r\x1b[K" + progress + " " + bar + " ", end="")
    print("\n", end="")


def main(args: Dict[str, str]):
    loop = asyncio.get_event_loop()

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

    if path := args["--file"]:
        print(f"Reading metadata from {path}.")
        with open(path, "rb") as f:
            raw_meta = f.read()
        meta = metadata.Metadata.from_bytes(raw_meta)
        params = tracker.Parameters.from_bytes(raw_meta)
    else:
        print("Downloading metadata from peers...", end="")
        info_hash, announce_list = magnet.parse(args["--magnet"])
        params = tracker.Parameters(info_hash)
        raw_meta = runners.run(loop, mex.Download(params, announce_list, max_peers))
        meta = metadata.Metadata.from_bytes(raw_meta)
        print("Done.")
        path = f"{info_hash.hex()}.torrent"
        print(f"Writing metadata to {path}.")
        with open(path, "wb") as f:
            f.write(raw_meta)

    print("Building the file tree...", end="")
    metadata.build_file_tree(meta.folder, meta.files)
    print("Done.")

    if folder := args["--folder"]:
        meta.folder = os.path.join(folder, meta.folder)
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
        download = base.Download(meta, params, outstanding, max_peers)
        download.tasks.add(
            loop.create_task(_print(max_peers, len(meta.pieces), download.poll))
        )
        runners.run(loop, download)


if __name__ == "__main__":
    main(docopt(__doc__))
