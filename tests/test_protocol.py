import asyncio
import os
import unittest

from surge import _metadata
from surge import messages
from surge import protocol
from surge.stream import Stream

from . import _tracker


async def upload(uploader_started, metadata):
    pieces = metadata.pieces
    info_hash = metadata.info_hash
    chunks = _metadata.make_chunks(pieces, metadata.files)
    store = []
    for piece in pieces:
        data = []
        for chunk in chunks[piece]:
            data.append(chunk.read())
        store.append(b"".join(data))

    async def _main(reader, writer):
        stream = Stream(reader, writer)
        received = await stream.read_handshake()
        if received.info_hash != info_hash:
            raise ValueError("Wrong 'info_hash'.")
        await stream.write(
            messages.Handshake(
                0,
                info_hash,
                b".\xbb\xde\x16\x08\xb0\xc9NK\x19[E\xf5g\xa9\x84!Z\xe5\x15",
            )
        )
        n = len(pieces)
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
        uploader_started.set()
        await server.serve_forever()


class TestProtocol(unittest.TestCase):
    def test_download(self):
        os.chdir(os.path.dirname(__file__))
        with open("example.torrent", "rb") as f:
            metadata = _metadata.Metadata.from_bytes(f.read())

        async def _main():
            tracker_started = asyncio.Event()
            asyncio.create_task(_tracker.serve_peers_http(tracker_started))
            uploader_started = asyncio.Event()
            asyncio.create_task(upload(uploader_started, metadata))
            await asyncio.gather(tracker_started.wait(), uploader_started.wait())
            missing_pieces = set(metadata.pieces)
            root = protocol.Root(
                metadata.info_hash,
                b"\xad6n\x84\xb3a\xa4\xc1\xa1\xde\xd4H\x01J\xc0]\x1b\x88\x92I",
                metadata.announce_list,
                metadata.pieces,
                missing_pieces,
                50,
                50,
            )
            async for piece, data in root.results:
                self.assertTrue(_metadata.valid_piece_data(piece, data))
                missing_pieces.remove(piece)
            self.assertFalse(missing_pieces)

        asyncio.run(_main())
