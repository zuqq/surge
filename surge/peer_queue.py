import asyncio

from . import actor
from . import tracker_protocol


class PeerQueue(actor.Actor):
    def __init__(self, metainfo, torrent_state):
        super().__init__()

        self._metainfo = metainfo
        self._torrent_state = torrent_state

        self._peers = asyncio.Queue()

    ### Actor implementation

    async def _main_coro(self):
        seen_peers = set()
        while True:
            resp = await tracker_protocol.request_peers(
                self._metainfo, self._torrent_state
            )
            print(f"Got {len(resp.peers)} peer(s) from {resp.announce}.")
            for peer in set(resp.peers) - seen_peers:
                self._peers.put_nowait(peer)
                seen_peers.add(peer)
            await asyncio.sleep(resp.interval)

    ### Queue interface

    async def get(self):
        """Return a peer that we have not yet connected to."""
        return await self._peers.get()

