from typing import List, Optional, Tuple

import dataclasses
import urllib.parse

import aiohttp

from . import bencoding
from . import metadata


@dataclasses.dataclass
class TrackerResponse:
    peers: Optional[List[metadata.Peer]] = None
    interval: Optional[int] = None
    error: Optional[str] = None

    @classmethod
    def from_bytes(cls, raw_resp):
        decoded_resp = bencoding.decode(raw_resp)
        if b"failure reason" in decoded_resp:
            return TrackerResponse(
                error=decoded_resp[b"failure reason"].decode("utf-8")
            )
        if isinstance(decoded_resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [metadata.Peer.from_dict(d) for d in decoded_resp[b"peers"]]
        else:
            # Binary model ("compact format") from BEP 23.
            raw_peers = decoded_resp[b"peers"]
            peers = []
            for i in range(0, len(raw_peers), 6):
                peers.append(metadata.Peer.from_bytes(raw_peers[i : i + 6]))
        return cls(peers, decoded_resp[b"interval"])


async def request_peers(metainfo):
    """Request peers from `metainfo.announce` and return a `TrackerResponse`."""
    tracker_params = (
        "info_hash",
        "peer_id",
        "port",
        "uploaded",
        "downloaded",
        "left",
        "event",
        "compact",
    )
    encoded_params = urllib.parse.urlencode(
        {param: getattr(metainfo, param) for param in tracker_params}
    )

    async with aiohttp.ClientSession() as session:
        async with session.get(metainfo.announce + "?" + encoded_params) as resp:
            raw_resp = await resp.read()

    return TrackerResponse.from_bytes(raw_resp)
