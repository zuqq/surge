from typing import List

import asyncio
import dataclasses
import enum
import secrets
import struct
import urllib.parse

import aiohttp

from . import bencoding
from . import metadata
from . import udp


def _parse_peers(raw_peers):
    peers = []
    for i in range(0, len(raw_peers), 6):
        peers.append(metadata.Peer.from_bytes(raw_peers[i : i + 6]))
    return peers


@dataclasses.dataclass
class TrackerResponse:
    announce: str
    interval: int
    peers: List[metadata.Peer]

    @classmethod
    def from_bytes(cls, announce, interval, raw_peers):
        return cls(announce, interval, _parse_peers(raw_peers))

    @classmethod
    def from_dict(cls, announce, resp):
        if isinstance(resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [metadata.Peer.from_dict(d) for d in resp[b"peers"]]
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


class TrackerMessage(enum.Enum):
    CONNECT = 0
    ANNOUNCE = 1


def _connect_request(trans_id):
    return struct.pack(">ql4s", 0x41727101980, TrackerMessage.CONNECT.value, trans_id)


def _announce_request(trans_id, conn_id, tracker_params):
    return struct.pack(
        ">8sl4s20s20sqqqlL4slH",
        conn_id,
        TrackerMessage.ANNOUNCE.value,
        trans_id,
        tracker_params.info_hash,
        tracker_params.peer_id,
        tracker_params.downloaded,
        tracker_params.left,
        tracker_params.uploaded,
        0,
        0,
        secrets.token_bytes(4),
        -1,
        6881,
    )


async def _request_peers_udp(url, tracker_params):
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        udp.DatagramStream,
        remote_addr=(url._replace(netloc=url.hostname).geturl()[6:], url.port),
    )
    trans_id = secrets.token_bytes(4)

    protocol.write(_connect_request(trans_id))
    await protocol.drain()

    data = await asyncio.wait_for(protocol.read(), timeout=1)
    _, _, conn_id = struct.unpack(">l4s8s", data)

    protocol.write(_announce_request(trans_id, conn_id, tracker_params))
    await protocol.drain()

    data = await asyncio.wait_for(protocol.read(), timeout=1)
    _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])

    transport.close()
    return (interval, data[20:])


async def request_peers(announce: str,
                        tracker_params: metadata.TrackerParameters) -> TrackerResponse:
    """Request peers from the URL `announce`."""
    url = urllib.parse.urlparse(announce)
    if url.scheme in ("http", "https"):
        resp = await _request_peers_http(url, tracker_params)
        return TrackerResponse.from_dict(announce, resp)
    elif url.scheme == "udp":
        interval, raw_peers = await _request_peers_udp(url, tracker_params)
        return TrackerResponse.from_bytes(announce, interval, raw_peers)
    else:
        raise ValueError(f"Announce needs to be HTTP/S or UDP.")
