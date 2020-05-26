from typing import List, Optional

import asyncio
import dataclasses
import enum
import hashlib
import secrets
import struct
import urllib.parse

import aiohttp

from . import bencoding
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


class Message(enum.Enum):
    CONNECT = 0
    ANNOUNCE = 1


def _connect_request(trans_id):
    return struct.pack(">ql4s", 0x41727101980, Message.CONNECT.value, trans_id)


def _announce_request(trans_id, conn_id, tracker_params):
    return struct.pack(
        ">8sl4s20s20sqqqlL4slH",
        conn_id,
        Message.ANNOUNCE.value,
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
    _, protocol = await loop.create_datagram_endpoint(
        udp.DatagramStream, remote_addr=(url.hostname, url.port)
    )

    trans_id = secrets.token_bytes(4)

    protocol.send(_connect_request(trans_id))
    await protocol.drain()

    data = await asyncio.wait_for(protocol.recv(), timeout=1)
    _, _, conn_id = struct.unpack(">l4s8s", data)

    protocol.send(_announce_request(trans_id, conn_id, tracker_params))
    await protocol.drain()

    data = await asyncio.wait_for(protocol.recv(), timeout=1)
    _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])

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
