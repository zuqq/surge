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


def touch(folder: str, files: Iterable[metadata.File]):
    for file in files:
        path = os.path.join(folder, file.path)
        tail, _ = os.path.split(path)
        if tail:
            os.makedirs(tail, exist_ok=True)
        with open(path, "a+b") as f:
            f.truncate(file.length)


def available(pieces: Iterable[metadata.Piece],
              files: Iterable[metadata.File],
              folder: str) -> Generator[metadata.Piece, None, None]:
    for piece in pieces:
        data = []
        for chunk in metadata.chunks(files, piece):
            path = os.path.join(folder, chunk.file.path)
            try:
                with open(path, "rb") as f:
                    f.seek(chunk.begin - chunk.file.begin)
                    data.append(f.read(chunk.length))
            except FileNotFoundError:
                continue
        if metadata.valid(piece, b"".join(data)):
            yield piece


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
    touch(meta.folder, meta.files)
    print("Done.")

    if args.folder:
        meta.folder = os.path.join(args.folder, meta.folder)
        print(f"Downloading to {meta.folder}.")

    outstanding = set(meta.pieces)

    if args.resume:
        print("Checking for available pieces...", end="")
        for piece in available(meta.pieces, meta.files, meta.folder):
            outstanding.remove(piece)
        print("Done.")

    runners.run(base.Download(meta, params, outstanding, max_peers=args.peers))


if __name__ == "__main__":
    main()
