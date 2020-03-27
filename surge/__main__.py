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

    with open(args.file, "rb") as f:
        raw_metainfo = f.read()
    print(f"Downloading {args.file}.")

    if args.resume:
        raise NotImplementedError("Resuming is not supported.")

    runners.run(torrent.Torrent(raw_metainfo))

    print("Exiting.")


if __name__ == "__main__":
    main()
