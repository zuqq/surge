import asyncio
import pathlib
import unittest

from surge import _metadata
from surge import bencoding
from surge import magnet
from surge import messages
from surge.stream import Stream

from . import _tracker


async def upload(uploader_started, info_hash, raw_info):
    peer_id = b".\xbb\xde\x16\x08\xb0\xc9NK\x19[E\xf5g\xa9\x84!Z\xe5\x15"

    async def _main(reader, writer):
        try:
            stream = Stream(reader, writer)
            received = await stream.read_handshake()
            if not received.reserved & messages.EXTENSION_PROTOCOL_BIT:
                raise ValueError("Extension protocol not supported.")
            if received.info_hash != info_hash:
                raise ValueError("Wrong 'info_hash'.")
            await stream.write(messages.Handshake(messages.EXTENSION_PROTOCOL_BIT, info_hash, peer_id))
            while True:
                received = await stream.read()
                if isinstance(received, messages.ExtensionHandshake):
                    ut_metadata = received.ut_metadata
                    break
            n = len(raw_info)
            await stream.write(messages.ExtensionHandshake(metadata_size=n))
            while True:
                try:
                    received = await stream.read()
                except asyncio.IncompleteReadError:
                    break
                if isinstance(received, messages.MetadataRequest):
                    i = received.index
                    k = i * magnet.PIECE_LENGTH
                    data = raw_info[k : k + magnet.PIECE_LENGTH]
                    await stream.write(messages.MetadataData(i, n, data, ut_metadata=ut_metadata))
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(_main, "127.0.0.1", 6881)
    async with server:
        uploader_started.set()
        await server.serve_forever()


class TestMagnet(unittest.TestCase):
    def test_parse(self):
        with self.subTest("valid"):
            info_hash, (announce,) = magnet.parse("magnet:?xt=urn:btih:be00b2943b4228bdae969ddae01e89c34932255e&tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce")
            self.assertEqual(info_hash, b"\xbe\x00\xb2\x94;B(\xbd\xae\x96\x9d\xda\xe0\x1e\x89\xc3I2%^")
            self.assertEqual(announce, "http://bttracker.debian.org:6969/announce")

        invalid = [
            "http://www.example.com",
            "magnet:?tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce",
            "magnet:?xt=aaa",
            "magnet:?xt=urn:btih:0",
            "magnet:?xt=urn:btih:86d4",
        ]

        for s in invalid:
            with self.subTest(s):
                with self.assertRaises(ValueError):
                    magnet.parse(s)

    def test_download(self):
        with (pathlib.Path() / "tests" / "example.torrent").open("rb") as f:
            raw_metadata = f.read()
        metadata = _metadata.Metadata.from_bytes(raw_metadata)
        info_hash = metadata.info_hash
        raw_info = bencoding.raw_val(raw_metadata, b"info")
        peer_id = b"\xad6n\x84\xb3a\xa4\xc1\xa1\xde\xd4H\x01J\xc0]\x1b\x88\x92I"
        announce_list = ["http://127.0.0.1:8080/announce"]
        max_peers = 50

        async def _main():
            tracker_started = asyncio.Event()
            uploader_started = asyncio.Event()
            tasks = {
                asyncio.create_task(_tracker.serve_peers_http(tracker_started)),
                asyncio.create_task(upload(uploader_started, info_hash, raw_info)),
            }
            await asyncio.gather(tracker_started.wait(), uploader_started.wait())
            actual = await magnet.download(info_hash, peer_id, announce_list, max_peers)
            self.assertEqual(_metadata.Metadata.from_bytes(actual), metadata)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run(_main())
