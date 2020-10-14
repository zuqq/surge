"""Parsers for tracker responses.

Supports compact peer lists as defined in [BEP 0023].

[BEP 0023]: http://www.bittorrent.org/beps/bep_0023.html
"""

from __future__ import annotations
from typing import Any, Dict, List

import dataclasses


@dataclasses.dataclass
class Parameters:
    info_hash: bytes
    peer_id: bytes
    port: int = 6881
    uploaded: int = 0
    downloaded: int = 0
    # This is initialized to 0 because we also need to connect to the tracker
    # before downloading the metadata.
    left: int = 0
    event: str = "started"
    compact: int = 1  # See BEP 23.


@dataclasses.dataclass(frozen=True)
class Peer:
    address: str
    port: int

    @classmethod
    def from_bytes(cls, bs: bytes) -> Peer:
        return cls(".".join(str(b) for b in bs[:4]), int.from_bytes(bs[4:], "big"))

    @classmethod
    def from_dict(cls, d: Dict[bytes, Any]) -> Peer:
        return cls(d[b"ip"].decode(), d[b"port"])


def _parse_peers(raw_peers):
    peers = []
    for i in range(0, len(raw_peers), 6):
        peers.append(Peer.from_bytes(raw_peers[i : i + 6]))
    return peers


@dataclasses.dataclass
class Result:
    interval: int
    peers: List[Peer]

    @classmethod
    def from_bytes(cls, interval: int, raw_peers: bytes) -> Result:
        return cls(interval, _parse_peers(raw_peers))

    @classmethod
    def from_dict(cls, resp: Dict[bytes, Any]) -> Result:
        if isinstance(resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [Peer.from_dict(d) for d in resp[b"peers"]]
        else:
            # Binary model ("compact format") from BEP 23.
            peers = _parse_peers(resp[b"peers"])
        return cls(resp[b"interval"], peers)
