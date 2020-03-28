import argparse
import logging
import os

from . import metadata
from . import runners
from . import torrent


def main():
    parser = argparse.ArgumentParser(
        description="Download files from the BitTorrent network."
    )
    parser.add_argument("file", help="path to the torrent file")
    parser.add_argument("--debug", help="enable logging", action="store_true")
    parser.add_argument("--resume", help="resume download", action="store_true")
    args = parser.parse_args()

    if args.debug:
        logfile = "debug.log"
        logging.basicConfig(
            level=logging.DEBUG,
            filename=logfile,
            filemode="w",
            format="%(asctime)s,%(msecs)03d %(levelname)s: %(message)s",
            datefmt="%H:%M:%S",
        )
        print(f"Logging to {logfile}.")

    print(f"Downloading {args.file}.")

    with open(args.file, "rb") as f:
        raw_metainfo = f.read()
    metainfo = metadata.Metainfo.from_bytes(raw_metainfo)
    torrent_state = metadata.TorrentState.from_bytes(raw_metainfo)

    if args.resume:
        print("Checking for available pieces.")
        available_pieces = metadata.available_pieces(
            metainfo.pieces, metainfo.files, metainfo.folder
        )
        print(f"Found {len(available_pieces)} valid pieces.")
    else:
        available_pieces = set()

    runners.run(torrent.Torrent(metainfo, torrent_state, available_pieces))

    print("Exiting.")


if __name__ == "__main__":
    main()
