from typing import Iterable

import asyncio
import dataclasses
import logging
import secrets
import urllib.parse

import aiohttp

from . import actor
from . import bencoding
from . import tracker
from . import tracker_protocol
from . import udp


class PeerQueue(actor.Supervisor):
    def __init__(
        self, announce_list: Iterable[str], tracker_params: tracker.Parameters,
    ):
        super().__init__()

        self._announce_list = announce_list
        self._tracker_params = tracker_params

        self._peers = asyncio.Queue()
        self._seen_peers = set()

    def __repr__(self):
        cls = self.__class__.__name__
        return f"<{cls} object at {hex(id(self))}>"

    ### Actor implementation

    async def _main(self):
        for announce in self._announce_list:
            url = urllib.parse.urlparse(announce)
            if url.scheme in ("http", "https"):
                await self.spawn_child(HTTPTrackerConnection(url, self._tracker_params))
            elif url.scheme == "udp":
                await self.spawn_child(UDPTrackerConnection(url, self._tracker_params))
            else:
                logging.warning("%r is invalid", announce)

    ### Queue interface

    async def get(self) -> tracker.Peer:
        """Return a fresh peer."""
        return await self._peers.get()

    def put_nowait(self, peer: tracker.Peer):
        if peer in self._seen_peers:
            return
        self._seen_peers.add(peer)
        self._peers.put_nowait(peer)


class _BaseTrackerConnection(actor.Actor):
    def __init__(
        self,
        url: urllib.parse.ParseResult,
        tracker_params: tracker.Parameters,
        *,
        max_tries: int = 5,
    ):
        super().__init__()

        self._url = url
        self._tracker_params = tracker_params
        self._max_tries = max_tries

    def __repr__(self):
        cls = self.__class__.__name__
        info = [f"url={repr(self._url.geturl())}"]
        return f"<{cls} object at {hex(id(self))} with {', '.join(info)}>"


class HTTPTrackerConnection(_BaseTrackerConnection):
    async def _main(self):
        while True:
            params = urllib.parse.parse_qs(self._url.query)
            params.update(dataclasses.asdict(self._tracker_params))
            req_url = self._url._replace(query=urllib.parse.urlencode(params))
            async with aiohttp.ClientSession() as session:
                async with session.get(req_url.geturl()) as req:
                    raw_resp = await req.read()
            d = bencoding.decode(raw_resp)
            if b"failure reason" in d:
                raise ConnectionError(d[b"failure reason"].decode())
            resp = tracker.Response.from_dict(self._url, d)
            for peer in resp.peers:
                self.parent.put_nowait(peer)
            await asyncio.sleep(resp.interval)


class UDPTrackerConnection(_BaseTrackerConnection):
    async def _request(self):
        loop = asyncio.get_running_loop()
        _, protocol = await loop.create_datagram_endpoint(
            udp.DatagramStream, remote_addr=(self._url.hostname, self._url.port)
        )

        trans_id = secrets.token_bytes(4)

        protocol.send(tracker_protocol.connect(trans_id))
        await protocol.drain()

        data = await asyncio.wait_for(protocol.recv(), timeout=5)
        _, _, conn_id = tracker_protocol.parse_connect(data)

        protocol.send(
            tracker_protocol.announce(trans_id, conn_id, self._tracker_params)
        )
        await protocol.drain()

        data = await asyncio.wait_for(protocol.recv(), timeout=5)
        _, _, interval, _, _ = tracker_protocol.parse_announce(data[:20])

        protocol.close()
        await protocol.wait_closed()

        return tracker.Response.from_bytes(self._url, interval, data[20:])

    async def _main(self):
        while True:
            tries = 0
            while tries < self._max_tries:
                try:
                    resp = await self._request()
                except Exception as e:
                    logging.warning("%r failed with %r", self._url.geturl(), e)
                    tries += 1
                else:
                    break
            else:
                raise ConnectionError("Tracker unreachable.")
            for peer in resp.peers:
                self.parent.put_nowait(peer)
            await asyncio.sleep(resp.interval)
