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


def parse_peers(raw_peers):
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
    def from_dict(cls, announce, resp):
        if isinstance(resp[b"peers"], list):
            # Dictionary model, as defined in BEP 3.
            peers = [metadata.Peer.from_dict(d) for d in resp[b"peers"]]
        else:
            # Binary model ("compact format") from BEP 23.
            peers = parse_peers(resp[b"peers"])
        return cls(announce, resp[b"interval"], peers)


class TrackerMessage(enum.Enum):
    CONNECT = 0
    ANNOUNCE = 1


def connect_request(trans_id):
    return struct.pack(">ql4s", 0x41727101980, TrackerMessage.CONNECT.value, trans_id)


def announce_request(trans_id, conn_id, torrent_state):
    return struct.pack(
        ">8sl4s20s20sqqqlL4slH",
        conn_id,
        TrackerMessage.ANNOUNCE.value,
        trans_id,
        torrent_state.info_hash,
        torrent_state.peer_id,
        torrent_state.downloaded,
        torrent_state.left,
        torrent_state.uploaded,
        0,
        0,
        secrets.token_bytes(4),
        -1,
        6881,
    )


async def request_peers(metainfo, torrent_state):
    """Request peers from the URLs in `metainfo.announce_list`, returning an
    instance of `TrackerResponse`."""

    for announce in metainfo.announce_list:
        url = urllib.parse.urlparse(announce)

        if url.scheme in ("http", "https"):
            params = urllib.parse.parse_qs(url.query)
            params.update(dataclasses.asdict(torrent_state))
            request_url = url._replace(query=urllib.parse.urlencode(params))
            async with aiohttp.ClientSession() as session:
                async with session.get(request_url.geturl()) as resp:
                    resp = bencoding.decode(await resp.read())
            if b"failure reason" in resp:
                raise ConnectionError(resp[b"failure reason"].decode())
            return TrackerResponse.from_dict(announce, resp)

        elif url.scheme == "udp":
            loop = asyncio.get_running_loop()
            transport, protocol = await loop.create_datagram_endpoint(
                udp.DatagramStream,
                remote_addr=(url._replace(netloc=url.hostname).geturl()[6:], url.port),
            )

            trans_id = secrets.token_bytes(4)

            protocol.write(connect_request(trans_id))
            await protocol.drain()

            data = await asyncio.wait_for(protocol.read(), timeout=1)
            _, _, conn_id = struct.unpack(">l4s8s", data)

            protocol.write(announce_request(trans_id, conn_id, torrent_state))
            await protocol.drain()

            data = await asyncio.wait_for(protocol.read(), timeout=1)
            _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])

            transport.close()

            return TrackerResponse(announce, interval, parse_peers(data[20:]))

        else:
            raise ValueError(f"Announce {announce} is not supported")
