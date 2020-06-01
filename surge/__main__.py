import argparse
import hashlib
import logging
import os


from . import base
from . import bencoding
from . import magnet
from . import metadata
from . import mex
from . import runners
from . import tracker


def touch(folder, files):
    for file in files:
        full_path = os.path.join(folder, file.path)
        tail, _ = os.path.split(full_path)
        if tail:
            os.makedirs(tail, exist_ok=True)
        with open(full_path, "a+b") as f:
            f.truncate(file.length)


def available(pieces, files, folder):
    result = set()
    chunks = metadata.chunks(pieces, files)
    for piece in pieces:
        data = []
        for c in chunks[piece]:
            file_path = os.path.join(folder, c.file.path)
            try:
                with open(file_path, "rb") as f:
                    f.seek(c.file_offset)
                    data.append(f.read(c.length))
            except FileNotFoundError:
                continue
        if hashlib.sha1(b"".join(data)).digest() == piece.hash:
            result.add(piece)
    return result


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
        outstanding -= available(meta.pieces, meta.files, meta.folder)
        print("Done.")

    runners.run(base.Download(meta, params, outstanding, max_peers=args.peers))


if __name__ == "__main__":
    main()
