from typing import List

import asyncio
import logging

from . import actor
from . import metadata
from . import tracker_protocol


class PeerQueue(actor.Actor):
    def __init__(
        self, announce_list: List[str], tracker_params: metadata.TrackerParameters
    ):
        super().__init__()

        self._stack = announce_list[::-1]
        self._tracker_params = tracker_params

        self._sleep = None
        self._peers = asyncio.Queue()

    ### Actor implementation

    async def _main_coro(self):
        seen_peers = set()
        while True:
            if not self._stack:
                raise RuntimeError("No trackers available.")
            announce = self._stack[-1]
            try:
                resp = await tracker_protocol.request_peers(
                    announce, self._tracker_params
                )
            except ConnectionError as e:
                logging.debug("Couldn't connect to %r: %r", announce, e)
                self._stack.pop()
            else:
                print(f"Got {len(resp.peers)} peer(s) from {resp.announce}.")
                for peer in set(resp.peers) - seen_peers:
                    self._peers.put_nowait(peer)
                    seen_peers.add(peer)
                self._sleep = asyncio.create_task(asyncio.sleep(resp.interval))
                await self._sleep

    ### Queue interface

    async def get(self) -> metadata.Peer:
        """Return a peer that we have not yet connected to."""
        try:
            peer = self._peers.get_nowait()
        except asyncio.QueueEmpty:
            if self._sleep is not None:
                self._sleep.cancel()
            peer = await self._peers.get()
        return peer
