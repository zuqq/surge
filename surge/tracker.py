from typing import List, Optional

import asyncio
import dataclasses
import hashlib
import secrets
import urllib.parse

import aiohttp

from . import bencoding
from . import tracker_protocol
from . import udp


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
    def from_bytes(cls, raw_metainfo):
        raw_info = bencoding.raw_val(raw_metainfo, b"info")
        info_hash = hashlib.sha1(raw_info).digest()
        return cls(info_hash)


@dataclasses.dataclass(eq=True, frozen=True)
class Peer:
    address: str
    port: int
    id: Optional[bytes] = None

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
    announce: str
    interval: int
    peers: List[Peer]

    @classmethod
    def from_bytes(cls, announce, interval, raw_peers):
        return cls(announce, interval, _parse_peers(raw_peers))

    @classmethod
    def from_dict(cls, announce, resp):
        if isinstance(resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [Peer.from_dict(d) for d in resp[b"peers"]]
        else:
            # Binary model ("compact format") from BEP 23.
            peers = _parse_peers(resp[b"peers"])
        return cls(announce, resp[b"interval"], peers)


async def _request_peers_http(url, tracker_params):
    params = urllib.parse.parse_qs(url.query)
    params.update(dataclasses.asdict(tracker_params))
    request_url = url._replace(query=urllib.parse.urlencode(params))
    async with aiohttp.ClientSession() as session:
        async with session.get(request_url.geturl()) as resp:
            resp = bencoding.decode(await resp.read())
    if b"failure reason" in resp:
        raise ConnectionError(resp[b"failure reason"].decode())
    return resp


async def _request_peers_udp(url, tracker_params):
    loop = asyncio.get_running_loop()
    _, protocol = await loop.create_datagram_endpoint(
        udp.DatagramStream, remote_addr=(url.hostname, url.port)
    )

    trans_id = secrets.token_bytes(4)

    protocol.send(tracker_protocol.connect(trans_id))
    await protocol.drain()

    data = await asyncio.wait_for(protocol.recv(), timeout=1)
    _, _, conn_id = tracker_protocol.parse_connect(data)

    protocol.send(tracker_protocol.announce(trans_id, conn_id, tracker_params))
    await protocol.drain()

    data = await asyncio.wait_for(protocol.recv(), timeout=1)
    _, _, interval, _, _ = tracker_protocol.parse_announce(data[:20])

    protocol.close()
    await protocol.wait_closed()

    return (interval, data[20:])


async def request_peers(announce: str, tracker_params: Parameters) -> Response:
    """Request peers from the URL `announce`."""
    url = urllib.parse.urlparse(announce)
    if url.scheme in ("http", "https"):
        resp = await _request_peers_http(url, tracker_params)
        return Response.from_dict(announce, resp)
    if url.scheme == "udp":
        interval, raw_peers = await _request_peers_udp(url, tracker_params)
        return Response.from_bytes(announce, interval, raw_peers)
    raise ValueError(f"Announce needs to be HTTP/S or UDP.")
