import asyncio
import pathlib
import unittest

from surge import _metadata
from surge import messages
from surge import protocol
from surge.channel import Channel
from surge.stream import Stream
from surge.tracker import Trackers

from . import _tracker


async def upload(uploader_started, metadata, folder):
    pieces = metadata.pieces
    info_hash = metadata.info_hash
    chunks = _metadata.make_chunks(pieces, metadata.files)
    store = []
    for piece in pieces:
        data = []
        for chunk in chunks[piece]:
            data.append(chunk.read(folder))
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
        folder = pathlib.Path() / "tests"
        with (folder / "example.torrent").open("rb") as f:
            raw_metadata = f.read()
        metadata = _metadata.Metadata.from_bytes(raw_metadata)

        async def _main():
            tracker_started = asyncio.Event()
            uploader_started = asyncio.Event()
            tasks = {
                asyncio.create_task(_tracker.serve_peers_http(tracker_started)),
                asyncio.create_task(upload(uploader_started, metadata, folder)),
            }
            await asyncio.gather(tracker_started.wait(), uploader_started.wait())
            info_hash = metadata.info_hash
            peer_id = b"\xad6n\x84\xb3a\xa4\xc1\xa1\xde\xd4H\x01J\xc0]\x1b\x88\x92I"
            async with Trackers(info_hash, peer_id, metadata.announce_list, 50) as trackers:
                pieces = metadata.pieces
                missing_pieces = set(metadata.pieces)
                max_peers = 50
                results = Channel(max_peers)
                torrent = protocol.Torrent(metadata.pieces, missing_pieces, results)
                for _ in range(max_peers):
                    tasks.add(
                        asyncio.create_task(
                            protocol.download_from_peer_loop(
                                torrent,
                                trackers,
                                info_hash,
                                peer_id,
                                pieces,
                                50,
                            )
                        )
                    )
                async for piece, data in results:
                    self.assertTrue(_metadata.valid_piece_data(piece, data))
                    missing_pieces.remove(piece)
                self.assertFalse(missing_pieces)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run(_main())
