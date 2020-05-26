import argparse
import logging
import os

from . import bencoding
from . import magnet
from . import metadata
from . import mex
from . import runners
from . import torrent
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
        print(f"Logging to {args.log}.")
    else:
        logging.disable(logging.CRITICAL)

    if args.file:
        print(f"Using metainfo file {args.file}.")
        with open(args.file, "rb") as f:
            raw_metainfo = f.read()
        tracker_params = tracker.Parameters.from_bytes(raw_metainfo)

    if args.magnet:
        print("Getting metainfo file from peers.")
        info_hash, announce_list = magnet.parse(args.magnet)
        tracker_params = tracker.Parameters(info_hash)
        info = runners.run(mex.Download(announce_list, tracker_params))
        # Peers only send us the raw value associated with the `b"info"` key,
        # so we still need to build the metainfo dictionary.
        raw_metainfo = b"".join(
            [
                b"d13:announce-list",
                bencoding.encode([[url.encode() for url in announce_list]]),
                b"4:info",
                info,
                b"e",
            ]
        )
        print("Got metainfo file.")

    metainfo = metadata.Metainfo.from_bytes(raw_metainfo)

    if args.folder:
        metainfo.folder = os.path.join(args.folder, metainfo.folder)
        print(f"Downloading to {metainfo.folder}.")

    if args.resume:
        print("Checking for available pieces.")
        available_pieces = metadata.available_pieces(
            metainfo.pieces, metainfo.files, metainfo.folder
        )
        print(f"Found {len(available_pieces)} valid pieces.")
    else:
        available_pieces = set()

    runners.run(
        torrent.Download(
            metainfo, tracker_params, available_pieces, max_peers=args.peers
        )
    )

    print("Done.")


if __name__ == "__main__":
    main()
