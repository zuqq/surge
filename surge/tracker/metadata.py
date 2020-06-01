from typing import List, Optional

import dataclasses
import hashlib
import secrets
import urllib.parse

from .. import bencoding


@dataclasses.dataclass
class Parameters:
    info_hash: bytes
    peer_id: bytes = secrets.token_bytes(20)
    port: int = 6881
    uploaded: int = 0
    downloaded: int = 0
    left: int = 0
    event: str = "started"
    compact: int = 1  # See BEP 23.

    @classmethod
    def from_bytes(cls, raw_meta):
        raw_info = bencoding.raw_val(raw_meta, b"info")
        info_hash = hashlib.sha1(raw_info).digest()
        return cls(info_hash)


@dataclasses.dataclass(eq=True, frozen=True)
class Peer:
    address: str
    port: int
    peer_id: Optional[bytes] = None

    @classmethod
    def from_bytes(cls, bs):
        return cls(".".join(str(b) for b in bs[:4]), int.from_bytes(bs[4:], "big"))

    @classmethod
    def from_dict(cls, d):
        return cls(d[b"ip"].decode(), d[b"port"], d[b"peer id"])


def _parse_peers(raw_peers):
    peers = []
    for i in range(0, len(raw_peers), 6):
        peers.append(Peer.from_bytes(raw_peers[i : i + 6]))
    return peers


@dataclasses.dataclass
class Response:
    url: urllib.parse.ParseResult
    interval: int
    peers: List[Peer]

    @classmethod
    def from_bytes(cls, url, interval, raw_peers):
        return cls(url, interval, _parse_peers(raw_peers))

    @classmethod
    def from_dict(cls, url, resp):
        if isinstance(resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [Peer.from_dict(d) for d in resp[b"peers"]]
        else:
            # Binary model ("compact format") from BEP 23.
            peers = _parse_peers(resp[b"peers"])
        return cls(url, resp[b"interval"], peers)
