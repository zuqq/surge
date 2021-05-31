import argparse
import asyncio
import secrets
import sys

try:
    import uvloop
except ImportError:
    pass
else:
    uvloop.install()

from . import _metadata
from . import protocol


def main(args):
    with open(args.file, "rb") as f:
        metadata = _metadata.Metadata.from_bytes(f.read())

    missing_pieces = set(metadata.pieces)

    if args.resume:
        for piece in _metadata.yield_available_pieces(metadata.pieces, metadata.files):
            missing_pieces.remove(piece)

    asyncio.run(
        protocol.download(
            metadata, secrets.token_bytes(20), missing_pieces, args.peers, args.requests
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download files from the BitTorrent network."
    )
    parser.add_argument(
        "file", help="Path to the .torrent file.", metavar="<file-path>"
    )
    parser.add_argument("--resume", help="Resume the download.", action="store_true")
    parser.add_argument(
        "--peers",
        help="Number of peers to connect to.",
        type=int,
        default=50,
        metavar="<peers>",
    )
    parser.add_argument(
        "--requests",
        help="Number of open requests per peer.",
        type=int,
        default=50,
        metavar="<requests>",
    )
    try:
        main(parser.parse_args())
    except KeyboardInterrupt:
        sys.exit(130)
