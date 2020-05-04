from typing import List

import asyncio
import logging

from . import actor
from . import tracker


class PeerQueue(actor.Actor):
    """Requests peers from the tracker and supplies them via the `get` method.

    Note that this class prematurely requests more peers if it runs out.
    """
    def __init__(
            self,
            announce_list: List[str],
            tracker_params: tracker.Parameters,
            *,
            max_tries: int = 5
        ):
        super().__init__()

        # Available trackers; unreachable or unresponsive trackers are discarded.
        self._stack = announce_list[::-1]
        self._tracker_params = tracker_params
        self._max_tries = max_tries

        self._sleep = None
        self._peers = asyncio.Queue()

    ### Actor implementation

    async def _main_coro(self):
        seen_peers = set()
        while True:
            if not self._stack:
                raise RuntimeError("No trackers available.")
            announce = self._stack[-1]
            tries = 0
            while tries < self._max_tries:
                try:
                    resp = await tracker.request_peers(announce, self._tracker_params)
                except Exception as e:
                    logging.debug("Couldn't connect to %r: %r", announce, e)
                    tries += 1
                else:
                    logging.debug("Got %r peers from %r.", len(resp.peers), announce)
                    for peer in set(resp.peers) - seen_peers:
                        self._peers.put_nowait(peer)
                        seen_peers.add(peer)
                    self._sleep = asyncio.create_task(asyncio.sleep(resp.interval))
                    await self._sleep
                    break
            else:
                logging.debug("Dropping %r.", announce)
                self._stack.pop()

    ### Queue interface

    async def get(self) -> tracker.Peer:
        """Return a fresh peer.

        If the internal peer supply is empty, request more from the tracker.
        """
        try:
            peer = self._peers.get_nowait()
        except asyncio.QueueEmpty:
            if self._sleep is not None:
                self._sleep.cancel()
            peer = await self._peers.get()
        return peer
