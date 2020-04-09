import argparse
import logging

from . import bencoding
from . import magnet
from . import metadata
from . import mex
from . import runners
from . import torrent


def main():
    parser = argparse.ArgumentParser(
        description="Download files from the BitTorrent network."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="torrent file")
    group.add_argument("--magnet", help="magnet link")
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

    if args.file:
        print(f"Using file {args.file}.")

        with open(args.file, "rb") as f:
            raw_metainfo = f.read()
        tracker_params = metadata.TrackerParameters.from_bytes(raw_metainfo)

    if args.magnet:
        print("Downloading metadata from peers.")

        info_hash, announce_list = magnet.parse(args.magnet)
        tracker_params = metadata.TrackerParameters(info_hash)
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

    metainfo = metadata.Metainfo.from_bytes(raw_metainfo)

    if args.resume:
        print("Checking for available pieces.")
        available_pieces = metadata.available_pieces(
            metainfo.pieces, metainfo.files, metainfo.folder
        )
        print(f"Found {len(available_pieces)} valid pieces.")
    else:
        available_pieces = set()

    runners.run(torrent.Download(metainfo, tracker_params, available_pieces))

    print("Done.")


if __name__ == "__main__":
    main()
