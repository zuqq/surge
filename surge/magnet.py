"""Implementation of the metadata exchange protocol.

Specification: [BEP 0009]

The metadata exchange protocol is a mechanism to exchange metadata (i.e.,
`.torrent` files) with peers. It uses the extension protocol to transmit its
messages as part of a BitTorrent connection.

[BEP 0009]: http://bittorrent.org/beps/bep_0009.html
"""

import argparse
import asyncio
import hashlib
import secrets
import sys
import urllib.parse

try:
    import uvloop
except ImportError:
    pass
else:
    uvloop.install()

from . import bencoding
from . import messages
from . import tracker
from .stream import open_stream


def parse(magnet_uri):
    """Return `(info_hash, announce_list)` of a magnet URI.

    Raise `ValueError` if `magnet_uri` is not a valid magnet URI.

    Specification: [BEP 009]

    [BEP 009]: http://www.bittorrent.org/beps/bep_0009.html
    """
    url = urllib.parse.urlparse(magnet_uri)
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
        raise ValueError("Invalid value for 'btih'.")
    announce_list = qs.get("tr", [])
    return info_hash, announce_list


def valid_raw_info(info_hash, raw_info):
    return hashlib.sha1(raw_info).digest() == info_hash


def assemble_raw_metadata(announce_list, raw_info):
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


# Length of a metadata piece.
PIECE_LENGTH = 2 ** 14


async def download_from_peer(root, peer, info_hash, peer_id):
    async with open_stream(peer) as stream:
        await stream.write(
            messages.Handshake(messages.EXTENSION_PROTOCOL_BIT, info_hash, peer_id)
        )
        received = await stream.read_handshake()
        if not received.reserved & messages.EXTENSION_PROTOCOL_BIT:
            raise ConnectionError("Extension protocol not supported.")
        if received.info_hash != info_hash:
            raise ConnectionError("Wrong 'info_hash'.")
        await stream.write(messages.ExtensionHandshake())
        while True:
            received = await stream.read()
            if isinstance(received, messages.ExtensionHandshake):
                ut_metadata = received.ut_metadata
                metadata_size = received.metadata_size
                break
        # Because the number of pieces is small, a simple stop-and-wait protocol
        # is fast enough.
        pieces = []
        for i in range((metadata_size + PIECE_LENGTH - 1) // PIECE_LENGTH):
            await stream.write(messages.MetadataRequest(i, ut_metadata=ut_metadata))
            while True:
                received = await stream.read()
                if isinstance(received, messages.MetadataData):
                    pieces.append(received.data)
                    break
        raw_info = b"".join(pieces)
        if valid_raw_info(info_hash, raw_info):
            root.put_result(raw_info)
        else:
            raise ConnectionError("Invalid data.")


async def download_from_peer_loop(root, info_hash, peer_id):
    while True:
        peer = await root.get_peer()
        try:
            return await download_from_peer(root, peer, info_hash, peer_id)
        except Exception:
            pass


class Root(tracker.TrackerMixin):
    def __init__(self, info_hash, peer_id, announce_list, max_peers):
        super().__init__(info_hash, peer_id, announce_list, max_peers)

        self.info_hash = info_hash
        self.peer_id = peer_id
        self.max_peers = max_peers

        self.result = asyncio.get_event_loop().create_future()

        self._tasks = set()

    def put_result(self, raw_info):
        if not self.result.done():
            self.result.set_result(assemble_raw_metadata(self.announce_list, raw_info))

    def start(self):
        super().start()
        for _ in range(self.max_peers):
            self._tasks.add(
                asyncio.create_task(
                    download_from_peer_loop(self, self.info_hash, self.peer_id)
                )
            )

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(super().stop(), *self._tasks, return_exceptions=True)
        self._tasks.clear()


async def download(info_hash, peer_id, announce_list, max_peers):
    """Download the `.torrent` file corresponding to `info_hash`."""
    root = Root(info_hash, peer_id, announce_list, max_peers)
    root.start()
    try:
        return await root.result
    finally:
        await root.stop()


def main(args):
    info_hash, announce_list = parse(args.uri)

    raw_metadata = asyncio.run(
        download(info_hash, secrets.token_bytes(20), announce_list, args.peers)
    )

    with open(f"{info_hash.hex()}.torrent", "wb") as f:
        f.write(raw_metadata)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download .torrent files from peers.")
    parser.add_argument("uri", help="The magnet URI to use.", metavar="<URI>")
    parser.add_argument(
        "--peers",
        help="Number of peers to connect to.",
        default=50,
        type=int,
        metavar="<peers>",
    )
    try:
        main(parser.parse_args())
    except KeyboardInterrupt:
        sys.exit(130)
