import asyncio
import contextlib
import os
import unittest

from surge import _metadata
from surge import messages
from surge import protocol
from surge.stream import Stream

from . import _tracker


async def upload(metadata):
    chunks = _metadata.make_chunks(metadata.pieces, metadata.files)
    store = []
    for piece in metadata.pieces:
        data = []
        for chunk in chunks[piece]:
            data.append(_metadata.read_chunk(chunk))
        store.append(b"".join(data))

    async def _main(reader, writer):
        stream = Stream(reader, writer)
        await stream.read_handshake()
        await stream.write(
            messages.Handshake(
                0,
                metadata.info_hash,
                b".\xbb\xde\x16\x08\xb0\xc9NK\x19[E\xf5g\xa9\x84!Z\xe5\x15",
            )
        )
        n = len(metadata.pieces)
        await stream.write(messages.Bitfield.from_indices(range(n), n))
        while True:
            try:
                received = await stream.read()
            except asyncio.IncompleteReadError:
                break
            if isinstance(received, messages.Interested):
                await stream.write(messages.Unchoke())
            elif isinstance(received, messages.Request):
                i = received.index
                k = received.begin
                await stream.write(
                    messages.Block(i, k, store[i][k : k + received.length])
                )

    server = await asyncio.start_server(_main, "127.0.0.1", 6881)
    async with server:
        await server.serve_forever()


class TestProtocol(unittest.TestCase):
    def test_download(self):
        os.chdir(os.path.dirname(__file__))
        with open("example.torrent", "rb") as f:
            metadata = _metadata.Metadata.from_bytes(f.read())

        async def _main():
            tasks = {
                asyncio.create_task(_tracker.serve_peers_http()),
                asyncio.create_task(upload(metadata)),
            }
            root = protocol.Root(
                metadata,
                b"\xad6n\x84\xb3a\xa4\xc1\xa1\xde\xd4H\x01J\xc0]\x1b\x88\x92I",
                set(metadata.pieces),
                50,
                50,
            )
            root.start()
            try:
                missing = set(metadata.pieces)
                async for piece, data in root.results:
                    self.assertTrue(_metadata.valid(piece, data))
                    missing.remove(piece)
                self.assertFalse(missing)
            finally:
                await root.stop()
                for task in tasks:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

        asyncio.run(_main())
