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
        ):
        super().__init__()

        # Available trackers; unreachable or unresponsive trackers are discarded.
        self._announce_list = announce_list
        self._tracker_params = tracker_params

        self._sleep = None
        self._peers = asyncio.Queue()

    async def _request_peers(self, announce, *, max_tries=5):
        tries = 0
        while tries < max_tries:
            try:
                resp = await tracker.request_peers(announce, self._tracker_params)
            except Exception as e:
                logging.warning("%r failed with %r", announce, e)
                tries += 1
            else:
                logging.info("%r sent %r peers", announce, len(resp.peers))
                return resp
        logging.warning("%r unreachable", announce)
        raise ConnectionError("Tracker unreachable.")

    ### Actor implementation

    async def _main_coro(self):
        seen_peers = set()
        while True:
            coros = []
            for announce in self._announce_list:
                coros.append(self._request_peers(announce))
            results = await asyncio.gather(*coros, return_exceptions=True)

            good_announces = []
            interval = 3600
            for result in results:
                if not isinstance(result, ConnectionError):
                    for peer in set(result.peers) - seen_peers:
                        self._peers.put_nowait(peer)
                        seen_peers.add(peer)
                    good_announces.append(result.announce)
                    interval = max(interval, result.interval)
            self._announce_list = good_announces

            self._sleep = asyncio.create_task(asyncio.sleep(interval))
            await self._sleep

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
