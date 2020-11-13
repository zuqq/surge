"""Download files from the BitTorrent network.

Usage:
    __main__.py <file> [--resume] [--peers <peers>] [--requests <requests>]
    __main__.py (-h | --help)

Options:
    <file>                 The .torrent file to use.
    --resume               Resume the download.
    --peers <peers>        Number of peers to connect to [default: 50].
    --requests <requests>  Number of open requests per peer [default: 50].
    -h, --help             Show this screen.

"""

import asyncio
import secrets
import sys

import docopt  # type: ignore
import uvloop  # type: ignore

from . import _metadata
from . import protocol


def main() -> None:
    args = docopt.docopt(__doc__)
    peer_id = secrets.token_bytes(20)
    max_peers = int(args["--peers"])
    max_requests = int(args["--requests"])
    with open(args["<file>"], "rb") as f:
        metadata = _metadata.Metadata.from_bytes(f.read())
    missing = set(metadata.pieces)

    if args["--resume"]:
        for piece in _metadata.available(metadata.pieces, metadata.files):
            missing.remove(piece)

    uvloop.install()
    asyncio.get_event_loop().run_until_complete(
        protocol.download(metadata, peer_id, missing, max_peers, max_requests)
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
