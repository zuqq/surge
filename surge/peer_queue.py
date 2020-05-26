from typing import Iterable

import asyncio
import logging

from . import actor
from . import tracker


class PeerQueue(actor.Supervisor):
    """Requests peers from trackers and supplies them via the `get` method."""

    def __init__(
            self, announces: Iterable[str], tracker_params: tracker.Parameters,
        ):
        super().__init__()

        # Available trackers; unreachable or unresponsive trackers are discarded.
        self._announces = set(announces)
        self._tracker_params = tracker_params

        self._peers = asyncio.Queue()
        self._seen_peers = set()

    ### Actor implementation

    async def _main_coro(self):
        for announce in self._announces:
            await self.spawn_child(
                TrackerConnection(self, announce, self._tracker_params)
            )

    async def _on_child_crash(self, child):
        if isinstance(child, TrackerConnection):
            self._announces.remove(child.announce)

    ### Queue interface

    async def get(self) -> tracker.Peer:
        """Return a fresh peer."""
        return await self._peers.get()

    def put_nowait(self, peer: tracker.Peer):
        if peer in self._seen_peers:
            return
        self._seen_peers.add(peer)
        self._peers.put_nowait(peer)


class TrackerConnection(actor.Actor):
    def __init__(
            self,
            peer_queue: PeerQueue,
            announce: str,
            tracker_params: tracker.Parameters,
            *,
            max_tries: int = 5
        ):
        super().__init__()

        self._peer_queue = peer_queue

        self.announce = announce

        self._tracker_params = tracker_params
        self._max_tries = max_tries

    async def _main_coro(self):
        while True:
            tries = 0
            while tries < self._max_tries:
                try:
                    resp = await tracker.request_peers(
                        self.announce, self._tracker_params
                    )
                except Exception as e:
                    logging.warning("%r failed with %r", self.announce, e)
                    tries += 1
                else:
                    logging.info("%r sent %r peers", self.announce, len(resp.peers))
                    break
            else:
                logging.warning("%r unreachable", self.announce)
                raise ConnectionError("Tracker unreachable.")
            for peer in resp.peers:
                self._peer_queue.put_nowait(peer)
            await asyncio.sleep(resp.interval)
