from typing import Generator, Iterable

import argparse
import logging
import os


from . import base
from . import bencoding
from . import magnet
from . import metadata
from . import mex
from . import runners
from . import tracker


def main():
    parser = argparse.ArgumentParser(
        description="Download files from the BitTorrent network."
    )
    parser.add_argument("--resume", help="resume download", action="store_true")
    parser.add_argument("--peers", help="number of peers", default=50, type=int)
    parser.add_argument("--folder", help="destination folder")
    parser.add_argument("--log", help="log file")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="torrent file")
    group.add_argument("--magnet", help="magnet link")
    args = parser.parse_args()

    if args.log:
        logging.basicConfig(
            level=logging.DEBUG,
            filename=args.log,
            filemode="w",
            format="%(asctime)s,%(msecs)03d %(levelname)s: %(message)s",
            datefmt="%H:%M:%S",
        )
    else:
        logging.disable(logging.CRITICAL)

    if args.file:
        print(f"Using metadata file {args.file}.")
        with open(args.file, "rb") as f:
            raw_meta = f.read()
        meta = metadata.Metadata.from_bytes(raw_meta)
        params = tracker.Parameters.from_bytes(raw_meta)

    if args.magnet:
        print("Getting metadata file from peers...", end="")
        info_hash, announce_list = magnet.parse(args.magnet)
        params = tracker.Parameters(info_hash)
        raw_info = runners.run(mex.Download(params, announce_list))
        if raw_info is None:
            return
        # Peers only send us the raw value associated with the `b"info"` key,
        # so we still need to build the metadata dictionary.
        meta = metadata.Metadata.from_dict(
            {
                b"announce-list": [[url.encode() for url in announce_list]],
                b"info": bencoding.decode(raw_info),
            }
        )
        print("Done.")

    print("Building the file tree...", end="")
    metadata.build_file_tree(meta.folder, meta.files)
    print("Done.")

    if args.folder:
        meta.folder = os.path.join(args.folder, meta.folder)
        print(f"Downloading to {meta.folder}.")

    outstanding = set(meta.pieces)

    if args.resume:
        print("Checking for available pieces...", end="")
        for piece in metadata.available_pieces(meta.pieces, meta.files, meta.folder):
            outstanding.remove(piece)
        print("Done.")

    runners.run(base.Download(meta, params, outstanding, max_peers=args.peers))


if __name__ == "__main__":
    main()
