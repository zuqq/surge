from typing import List, Tuple

import urllib.parse


def parse(magnet: str) -> Tuple[bytes, List[str]]:
    url = urllib.parse.urlparse(magnet)
    qs = urllib.parse.parse_qs(url.query)
    (xt,) = qs["xt"]
    # Strip "urn:btih:".
    info_hash = bytes.fromhex(xt[9:])
    announce_list = qs.get("tr", [])

    return info_hash, announce_list
