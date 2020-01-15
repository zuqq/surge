import urllib.parse

import aiohttp

import bencoding
import metadata


async def _get_peers_http(metainfo):
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
            decoded_resp = bencoding.decode(await resp.read())
    if isinstance(decoded_resp[b"peers"], list):
        # Dictionary model, as defined in BEP 3.
        peers = [metadata.Peer.from_dict(d) for d in decoded_resp[b"peers"]]
    else:
        # Binary model ("compact format") from BEP 23.
        raw_peers = decoded_resp[b"peers"]
        peers = []
        for i in range(0, len(raw_peers), 6):
            peers.append(metadata.Peer.from_bytes(raw_peers[i : i + 6]))
    return peers, decoded_resp[b"interval"]


async def get_peers(metainfo):
    if metainfo.announce.startswith("http"):
        return await _get_peers_http(metainfo)
    else:
        raise ValueError("Non-HTTP announce.")
