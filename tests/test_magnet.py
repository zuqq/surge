import asyncio
import os
import unittest

from surge import _metadata
from surge import bencoding
from surge import magnet
from surge import messages
from surge.stream import Stream

from . import _tracker


async def upload(info_hash, raw_info):
    async def _main(reader, writer):
        stream = Stream(reader, writer)
        await stream.read_handshake()
        await stream.write(
            messages.Handshake(
                1 << 20,
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
        await server.serve_forever()


class TestMagnet(unittest.TestCase):
    def test_parse(self):
        with self.subTest("valid"):
            self.assertEqual(
                magnet.parse(
                    "magnet:?xt=urn:btih:86d4c80024a469be4c50bc5a102cf71780310074"
                    "&dn=debian-10.2.0-amd64-netinst.iso"
                    "&tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce"
                ),
                (
                    b"\x86\xd4\xc8\x00$\xa4i\xbeLP\xbcZ\x10,\xf7\x17\x801\x00t",
                    ["http://bttracker.debian.org:6969/announce"],
                ),
            )

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
            tasks = {
                asyncio.create_task(_tracker.serve_peers_http()),
                asyncio.create_task(upload(info_hash, raw_info)),
            }
            root = magnet.Root(
                info_hash,
                ["http://127.0.0.1:8080/announce"],
                b"\xad6n\x84\xb3a\xa4\xc1\xa1\xde\xd4H\x01J\xc0]\x1b\x88\x92I",
                50,
            )
            root.start()
            try:
                result = _metadata.Metadata.from_bytes(await root.result)
                self.assertEqual(metadata, result)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, root.stop(), return_exceptions=True)

        asyncio.run(_main())
