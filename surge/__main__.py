"""Download files from the BitTorrent network.

Usage:
    __main__.py --magnet URI [--peers PEERS]
    __main__.py FILE [--resume] [--peers PEERS] [--requests REQUESTS]
    __main__.py (-h | --help)

Options:
    -h, --help          Show this screen.
    --resume            Resume the download.
    --peers PEERS       Number of peers to connect to [default: 50].
    --requests REQUEST  Number of open requests per peer [default: 50].

"""

import asyncio
import secrets
import sys

import docopt  # type: ignore
import uvloop  # type: ignore

from . import _metadata
from . import magnet
from . import mex
from . import protocol


def main() -> None:
    uvloop.install()
    loop = asyncio.get_event_loop()

    args = docopt.docopt(__doc__)
    peer_id = secrets.token_bytes(20)
    max_peers = int(args["--peers"])

    if args["--magnet"]:
        info_hash, announce_list = magnet.parse(args["URI"])

        print("Downloading .torrent file from peers.")
        raw_metadata = loop.run_until_complete(
            mex.download(info_hash, announce_list, peer_id, max_peers)
        )

        path = f"{info_hash.hex()}.torrent"
        print(f"Saving .torrent file to {path}.")
        with open(path, "wb") as f:
            f.write(raw_metadata)
    else:
        with open(args["FILE"], "rb") as f:
            metadata = _metadata.Metadata.from_bytes(f.read())
        missing = set(metadata.pieces)
        max_requests = int(args["--requests"])

        if args["--resume"]:
            print("Checking for available pieces.")
            for piece in _metadata.available(metadata.pieces, metadata.files):
                missing.remove(piece)

        loop.run_until_complete(
            protocol.download(metadata, peer_id, missing, max_peers, max_requests)
        )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
