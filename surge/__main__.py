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

    runners.run(torrent.Torrent(metainfo))

    print("Exiting.")


if __name__ == "__main__":
    main()
