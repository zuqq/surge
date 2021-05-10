import asyncio
import os
import unittest

from surge import _metadata
from surge import bencoding
from surge import magnet
from surge import messages
from surge.stream import Stream

from . import _tracker


async def upload(uploader_started, info_hash, raw_info):
    extension_protocol = 1 << 20

    async def _main(reader, writer):
        stream = Stream(reader, writer)
        received = await stream.read_handshake()
        if not received.reserved & extension_protocol:
            raise ValueError("Extension protocol not supported.")
        if received.info_hash != info_hash:
            raise ValueError("Wrong 'info_hash'.")
        await stream.write(
            messages.Handshake(
                extension_protocol,
                info_hash,
                b".\xbb\xde\x16\x08\xb0\xc9NK\x19[E\xf5g\xa9\x84!Z\xe5\x15",
            )
        )
        while True:
            received = await stream.read()
            if isinstance(received, messages.ExtensionHandshake):
                ut_metadata = received.ut_metadata
                break
        n = len(raw_info)
        await stream.write(messages.ExtensionHandshake(metadata_size=n))
        piece_length = 2 ** 14
        while True:
            try:
                received = await stream.read()
            except asyncio.IncompleteReadError:
                break
            if isinstance(received, messages.MetadataRequest):
                i = received.index
                k = i * piece_length
                await stream.write(
                    messages.MetadataData(
                        i, n, raw_info[k : k + piece_length], ut_metadata=ut_metadata
                    )
                )

    server = await asyncio.start_server(_main, "127.0.0.1", 6881)
    async with server:
        uploader_started.set()
        await server.serve_forever()


class TestMagnet(unittest.TestCase):
    def test_parse(self):
        with self.subTest("valid"):
            info_hash, (announce,) = magnet.parse(
                "magnet:?xt=urn:btih:be00b2943b4228bdae969ddae01e89c34932255e"
                "&tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce"
            )

            self.assertEqual(
                info_hash,
                b"\xbe\x00\xb2\x94;B(\xbd\xae\x96\x9d\xda\xe0\x1e\x89\xc3I2%^",
            )
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
        os.chdir(os.path.dirname(__file__))
        with open("example.torrent", "rb") as f:
            raw_metadata = f.read()
        metadata = _metadata.Metadata.from_bytes(raw_metadata)
        info_hash = metadata.info_hash
        raw_info = bencoding.raw_val(raw_metadata, b"info")

        async def _main():
            tracker_started = asyncio.Event()
            asyncio.create_task(_tracker.serve_peers_http(tracker_started))
            uploader_started = asyncio.Event()
            asyncio.create_task(upload(uploader_started, info_hash, raw_info))
            await asyncio.gather(tracker_started.wait(), uploader_started.wait())
            actual = await magnet.download(
                info_hash,
                ["http://127.0.0.1:8080/announce"],
                b"\xad6n\x84\xb3a\xa4\xc1\xa1\xde\xd4H\x01J\xc0]\x1b\x88\x92I",
                50,
            )
            self.assertEqual(_metadata.Metadata.from_bytes(actual), metadata)

        asyncio.run(_main())
