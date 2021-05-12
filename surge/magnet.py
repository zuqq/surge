import argparse
import asyncio
import hashlib
import secrets
import urllib.parse

from . import bencoding
from . import messages
from . import tracker
from .stream import open_stream


def parse(magnet_uri):
    """Return the info hash and announce list of a magnet URI.

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
        raise ValueError("Invalid info hash.")
    announce_list = qs.get("tr", [])
    return info_hash, announce_list


def main(args):
    info_hash, announce_list = parse(args.uri)
    peer_id = secrets.token_bytes(20)

    try:
        import uvloop
    except ImportError:
        pass
    else:
        uvloop.install()

    raw_metadata = asyncio.run(download(info_hash, peer_id, announce_list, args.peers))

    path = f"{info_hash.hex()}.torrent"
    with open(path, "wb") as f:
        f.write(raw_metadata)


async def download(info_hash, peer_id, announce_list, max_peers):
    """Return the content of the `.torrent` file."""
    root = Root(info_hash, peer_id, announce_list, max_peers)
    return await root.result


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


class Root:
    """Implementation of the metadata exchange protocol.

    Specification: [BEP 0009]

    The metadata exchange protocol is a mechanism to exchange metadata (i.e.,
    `.torrent` files) with peers. It uses the extension protocol to transmit its
    messages as part of a BitTorrent connection.

    [BEP 0009]: http://bittorrent.org/beps/bep_0009.html
    """

    def __init__(self, info_hash, peer_id, announce_list, max_peers):
        self.info_hash = info_hash
        self.announce_list = announce_list
        self.peer_id = peer_id
        self.max_peers = max_peers

        # Future that will hold the metadata.
        self.result = asyncio.get_event_loop().create_future()

        self._tracker_root = tracker.Root(
            self, info_hash, peer_id, announce_list, max_peers
        )

        self._nodes = set()

    def maybe_add_node(self):
        if len(self._nodes) < self.max_peers and self._tracker_root.new_peers:
            self._nodes.add(
                asyncio.create_task(
                    download_from_peer(
                        self,
                        self._tracker_root.get_peer(),
                        self.info_hash,
                        self.peer_id,
                    )
                )
            )

    def put_result(self, raw_info):
        if not self.result.done():
            self.result.set_result(assemble_raw_metadata(self.announce_list, raw_info))

    def remove_node(self, task):
        self._nodes.remove(task)
        self.maybe_add_node()


async def download_from_peer(root, peer, info_hash, peer_id):
    try:
        async with open_stream(peer) as stream:
            extension_protocol = 1 << 20
            await stream.write(
                messages.Handshake(extension_protocol, info_hash, peer_id)
            )
            received = await asyncio.wait_for(stream.read_handshake(), 30)
            if not received.reserved & extension_protocol:
                raise ConnectionError()("Extension protocol not supported.")
            if received.info_hash != info_hash:
                raise ConnectionError("Wrong 'info_hash'.")
            await stream.write(messages.ExtensionHandshake())
            while True:
                received = await asyncio.wait_for(stream.read(), 30)
                if isinstance(received, messages.ExtensionHandshake):
                    ut_metadata = received.ut_metadata
                    metadata_size = received.metadata_size
                    break
            # Because the number of pieces is small, a simple stop-and-wait
            # protocol is fast enough.
            piece_length = 2 ** 14
            pieces = []
            for i in range((metadata_size + piece_length - 1) // piece_length):
                await stream.write(messages.MetadataRequest(i, ut_metadata=ut_metadata))
                while True:
                    received = await asyncio.wait_for(stream.read(), 30)
                    if isinstance(received, messages.MetadataData):
                        pieces.append(received.data)
                        break
            raw_info = b"".join(pieces)
            if valid_raw_info(info_hash, raw_info):
                root.put_result(raw_info)
            else:
                raise ConnectionError("Invalid data.")
    except Exception:
        pass
    finally:
        root.remove_node(asyncio.current_task())


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
    main(parser.parse_args())
