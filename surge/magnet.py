"""Magnet URI parser.

Specification: [BEP 009]

[BEP 009]: http://www.bittorrent.org/beps/bep_0009.html
"""

from typing import List, Tuple

import urllib.parse


def parse(magnet: str) -> Tuple[bytes, List[str]]:
    """Parse a magnet link.

    Raise `ValueError` if `magnet` is not a valid magnet link.
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
