"""Download .torrent files from peers.

Usage:
    magnet.py <URI> [--peers=<peers>]
    magnet.py (-h | --help)

Options:
    <URI>             The magnet URI to use.
    --peers=<peers>   Number of peers to connect to [default: 50].
    -h, --help        Show this screen.

"""

import asyncio
import itertools
import hashlib
import secrets
import sys
import urllib.parse

import docopt
import uvloop

from . import bencoding
from . import messages
from . import tracker
from .stream import open_stream


def parse(magnet):
    """Parse a magnet URI.

    Raise `ValueError` if `magnet` is not a valid magnet link.

    Specification: [BEP 009]

    [BEP 009]: http://www.bittorrent.org/beps/bep_0009.html
    """
    url = urllib.parse.urlparse(magnet)
    qs = urllib.parse.parse_qs(url.query)
    if url.scheme != "magnet":
        raise ValueError("Invalid scheme.")
    if "xt" not in qs:
        raise ValueError("Missing key 'xt'.")
    (xt,) = qs["xt"]
    if not xt.startswith("urn:btih:"):
        raise ValueError("Invalid value for 'xt'.")
    info_hash = bytes.fromhex(xt[9:])
    if len(info_hash) != 20:
        raise ValueError("Invalid info hash.")
    announce_list = qs.get("tr", [])
    return info_hash, announce_list


def main(args):
    peer_id = secrets.token_bytes(20)
    max_peers = int(args["--peers"])
    info_hash, announce_list = parse(args["<URI>"])

    uvloop.install()
    raw_metadata = asyncio.run(download(info_hash, announce_list, peer_id, max_peers))

    path = f"{info_hash.hex()}.torrent"
    with open(path, "wb") as f:
        f.write(raw_metadata)


async def download(info_hash, announce_list, peer_id, max_peers):
    """Return the content of the `.torrent` file."""
    root = Root(info_hash, announce_list, peer_id, max_peers)
    root.start()
    try:
        return await root.result
    finally:
        await root.stop()


def valid(info_hash, raw_info):
    """Check that the `b"info"` value is valid."""
    return hashlib.sha1(raw_info).digest() == info_hash


def assemble(announce_list, raw_info):
    """Build the metadata from list of trackers and the `b"info"` value."""
    # We can't just decode and re-encode, because the value associated with
    # the key `b"info"` needs to be preserved exactly.
    return b"".join(
        (
            b"d",
            b"13:announce-list",
            bencoding.encode([[url.encode() for url in announce_list]]),
            b"4:info",
            raw_info,
            b"e",
        )
    )


class Root:
    """Implementation of the metadata exchange protocol.

    Specification: [BEP 0009]

    The metadata exchange protocol is a mechanism to exchange metadata (i.e.,
    `.torrent` files) with peers. It uses the extension protocol to transmit its
    messages as part of a BitTorrent connection; see `messages.MetadataMessage`.
    Therefore the implementation uses the same approach as that of the main
    protocol.

    [BEP 0009]: http://bittorrent.org/beps/bep_0009.html
    """

    def __init__(self, info_hash, announce_list, peer_id, max_peers):
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.max_peers = max_peers

        self._announce_list = announce_list
        self._trackers = set()
        self._seen_peers = set()
        self._new_peers = asyncio.Queue(max_peers)

        self._nodes = set()

        # Future that will hold the metadata.
        self.result = asyncio.get_event_loop().create_future()

    async def put_peer(self, peer):
        if peer in self._seen_peers:
            return
        self._seen_peers.add(peer)
        await self._new_peers.put(peer)
        self.maybe_add_node()

    def remove_tracker(self, task):
        self._trackers.remove(task)

    def maybe_add_node(self):
        if len(self._nodes) < self.max_peers and self._new_peers.qsize():
            peer = self._new_peers.get_nowait()
            self._nodes.add(asyncio.create_task(download_from_peer(self, peer)))

    def remove_node(self, task):
        self._nodes.remove(task)
        self.maybe_add_node()

    def start(self):
        parameters = tracker.Parameters(self.info_hash, self.peer_id)
        for announce in self._announce_list:
            url = urllib.parse.urlparse(announce)
            if url.scheme in ("http", "https"):
                coroutine = tracker.request_peers_http(self, url, parameters)
            elif url.scheme == "udp":
                coroutine = tracker.request_peers_udp(self, url, parameters)
            else:
                raise ValueError("Unsupported announce.")
            self._trackers.add(asyncio.create_task(coroutine))

    async def stop(self):
        for task in itertools.chain(self._trackers, self._nodes):
            task.cancel()
        await asyncio.gather(*self._trackers, *self._nodes, return_exceptions=True)

    def put_result(self, raw_info):
        if not self.result.done():
            self.result.set_result(assemble(self._announce_list, raw_info))


async def download_from_peer(root, peer):
    try:
        async with open_stream(peer) as stream:
            info_hash = root.info_hash
            peer_id = root.peer_id
            extension_protocol = 1 << 20
            await stream.write(
                messages.Handshake(extension_protocol, info_hash, peer_id)
            )
            received = await asyncio.wait_for(stream.read_handshake(), 30)
            if not received.reserved & extension_protocol:
                raise ValueError("Extension protocol not supported.")
            if received.info_hash != info_hash:
                raise ValueError("Wrong 'info_hash'.")
            await stream.write(messages.ExtensionHandshake())
            while True:
                received = await asyncio.wait_for(stream.read(), 30)
                if isinstance(received, messages.ExtensionHandshake):
                    ut_metadata = received.ut_metadata
                    metadata_size = received.metadata_size
                    break
            # The metadata is partitioned into pieces of size `2 ** 14`, except for the
            # last piece which may be smaller. The peer knows this partition, so we only
            # need to tell it the indices of the pieces that we want. Because the total
            # number of pieces is typically very small, a simple stop-and-wait protocol
            # is fast enough.
            piece_length = 2 ** 14
            pieces = []
            for i in range((metadata_size + piece_length - 1) // piece_length):
                await stream.write(messages.MetadataRequest(i, ut_metadata))
                while True:
                    received = await asyncio.wait_for(stream.read(), 30)
                    if isinstance(received, messages.MetadataData):
                        # We assume that the peer sends us data for the piece that we
                        # just requested; if not, the result of the transaction will be
                        # invalid. This assumption is reasonable because we request one
                        # piece at a time.
                        pieces.append(received.data)
                        break
            raw_info = b"".join(pieces)
            if valid(info_hash, raw_info):
                root.put_result(raw_info)
            else:
                raise ValueError("Invalid data.")
    except Exception:
        pass
    finally:
        root.remove_node(asyncio.current_task())


if __name__ == "__main__":
    try:
        main(docopt.docopt(__doc__))
    except KeyboardInterrupt:
        sys.exit(130)
