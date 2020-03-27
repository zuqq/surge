from typing import List, Optional, Tuple

import dataclasses
import urllib.parse

import aiohttp

from . import bencoding
from . import metadata


@dataclasses.dataclass
class TrackerResponse:
    announce: str
    interval: int
    peers: List[metadata.Peer]

    @classmethod
    def from_dict(cls, announce, resp):
        if isinstance(resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [metadata.Peer.from_dict(d) for d in resp[b"peers"]]
        else:
            # Binary model ("compact format") from BEP 23.
            raw_peers = resp[b"peers"]
            peers = []
            for i in range(0, len(raw_peers), 6):
                peers.append(metadata.Peer.from_bytes(raw_peers[i : i + 6]))
        return cls(announce, resp[b"interval"], peers)


async def request_peers(metainfo, torrent_state):
    """Request peers from the URLs in `metainfo.announce`, returning an
    instance of `TrackerResponse`."""
    encoded_params = urllib.parse.urlencode(dataclasses.asdict(torrent_state))

    async with aiohttp.ClientSession() as session:
        for announce in metainfo.announce_list:
            async with session.get(announce + "?" + encoded_params) as resp:
                resp = bencoding.decode(await resp.read())
            if b"failure reason" in resp:
                raise ConnectionError(resp[b"failure reason"].decode())
            return TrackerResponse.from_dict(announce, resp)
