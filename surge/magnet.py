"""Download .torrent files from peers.

Usage:
    magnet.py <URI> [--peers=<peers>]
    magnet.py (-h | --help)

Options:
    <URI>             The magnet URI to use.
    --peers=<peers>   Number of peers to connect to [default: 50].
    -h, --help        Show this screen.

"""

from typing import List, Tuple

import asyncio
import secrets
import sys
import urllib.parse

from . import mex

import docopt  # type: ignore
import uvloop  # type: ignore


def parse(magnet: str) -> Tuple[bytes, List[str]]:
    """Parse a magnet URI.

    Raise `ValueError` if `magnet` is not a valid magnet link.

    Specification: [BEP 009]

    [BEP 009]: http://www.bittorrent.org/beps/bep_0009.html
    """
    url = urllib.parse.urlparse(magnet)
    qs = urllib.parse.parse_qs(url.query)
    if url.scheme != "magnet":
        raise ValueError("Invalid scheme.")
    if "xt" not in qs:
        raise ValueError("Missing key 'xt'.")
    (xt,) = qs["xt"]
    if not xt.startswith("urn:btih:"):
        raise ValueError("Invalid value for 'xt'.")
    info_hash = bytes.fromhex(xt[9:])
    if len(info_hash) != 20:
        raise ValueError("Invalid info hash.")
    announce_list = qs.get("tr", [])
    return info_hash, announce_list


def main() -> None:
    args = docopt.docopt(__doc__)
    peer_id = secrets.token_bytes(20)
    max_peers = int(args["--peers"])
    info_hash, announce_list = parse(args["<URI>"])

    uvloop.install()
    raw_metadata = asyncio.run(
        mex.download(info_hash, announce_list, peer_id, max_peers)
    )

    path = f"{info_hash.hex()}.torrent"
    with open(path, "wb") as f:
        f.write(raw_metadata)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
