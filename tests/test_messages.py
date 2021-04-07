import hashlib
import struct
import unittest

from surge import _metadata
from surge import bencoding
from surge import messages


def _extension_message(extension_value, payload):
    n = len(payload)
    return struct.pack(f">LBB{n}s", n + 2, 20, extension_value, payload)


class TestMessages(unittest.TestCase):
    reserved = 1 << 20
    info_hash = b"O\xd2\xd3Y\x8a\x11\x01\xa1U\xdd\x86|\x91\x04\xfc\xd2\xd9\xe4$+"
    peer_id = b"\xa7\x88\x06\x8b\xeb6i~=//\x1e\xc8\x1d\xbb\x12\x023\xa58"
    handshake_reference = struct.pack(
        ">B19sQ20s20s", 19, b"BitTorrent protocol", reserved, info_hash, peer_id
    )
    handshake = messages.Handshake(reserved, info_hash, peer_id)

    valid = []

    valid.append((struct.pack(">L", 0), messages.Keepalive()))

    valid.append((struct.pack(">LB", 1, 0), messages.Choke()))

    valid.append((struct.pack(">LB", 1, 1), messages.Unchoke()))

    valid.append((struct.pack(">LB", 1, 2), messages.Interested()))

    valid.append((struct.pack(">LB", 1, 3), messages.NotInterested()))

    index = 0
    valid.append((struct.pack(">LBL", 5, 4, index), messages.Have(index)))

    indices = {0}
    bitfield = messages.Bitfield.from_indices(indices, len(indices))
    valid.append((struct.pack(">LBB", 2, 5, 1 << 7), bitfield))

    data = b"a"
    begin = 0
    length = len(data)
    piece = _metadata.Piece(index, begin, length, hashlib.sha1(data).digest())
    block = _metadata.Block(piece, begin, length)

    valid.append(
        (
            struct.pack(">LBLLL", 13, 6, block.piece.index, block.begin, block.length),
            messages.Request.from_block(block),
        )
    )

    n = len(data)
    valid.append(
        (
            struct.pack(f">LBLL{n}s", n + 9, 7, block.piece.index, block.begin, data),
            messages.Block.from_block(block, data),
        )
    )

    valid.append(
        (
            struct.pack(">LBLLL", 13, 8, block.piece.index, block.begin, block.length),
            messages.Cancel.from_block(block),
        )
    )

    raw_info = bencoding.encode(
        {
            b"length": len(data),
            b"name": data,
            b"piece length": 2 ** 18,
            b"pieces": piece.hash,
        }
    )
    ut_metadata = 3
    metadata_size = len(raw_info)

    valid.append(
        (
            _extension_message(
                0,
                bencoding.encode(
                    {
                        b"m": {b"ut_metadata": ut_metadata},
                        b"metadata_size": metadata_size,
                    }
                ),
            ),
            messages.ExtensionHandshake(ut_metadata, metadata_size),
        )
    )

    index = 0
    valid.append(
        (
            _extension_message(
                ut_metadata,
                b"".join(
                    (
                        bencoding.encode(
                            {
                                b"msg_type": 1,
                                b"piece": index,
                                b"total_size": metadata_size,
                            }
                        ),
                        raw_info,
                    )
                ),
            ),
            messages.MetadataData(
                index, metadata_size, raw_info, ut_metadata=ut_metadata
            ),
        )
    )

    index = 0
    valid.append(
        (
            _extension_message(
                ut_metadata, bencoding.encode({b"msg_type": 2, b"piece": index})
            ),
            messages.MetadataReject(index, ut_metadata=ut_metadata),
        )
    )

    index = 0
    valid.append(
        (
            _extension_message(
                ut_metadata, bencoding.encode({b"msg_type": 0, b"piece": index})
            ),
            messages.MetadataRequest(index, ut_metadata=ut_metadata),
        )
    )

    def test_to_bytes(self):
        with self.subTest(self.handshake):
            self.assertEqual(self.handshake.to_bytes(), self.handshake_reference)

        for x, y in self.valid:
            with self.subTest(y):
                self.assertEqual(y.to_bytes(), x)

    def test_to_indices(self):
        self.assertEqual(self.bitfield.to_indices(), self.indices)

    def test_parse_handshake(self):
        self.assertEqual(
            messages.parse_handshake(self.handshake_reference), self.handshake
        )

    def test_parse(self):
        for x, y in self.valid:
            with self.subTest(x):
                self.assertEqual(messages.parse(x), y)

        with self.subTest("Missing prefix."):
            with self.assertRaises(ValueError):
                messages.parse(struct.pack(">B", 0))

        with self.subTest("Wrong prefix."):
            with self.assertRaises(ValueError):
                messages.parse(struct.pack(">LB", 0, 0))

        with self.subTest("Unknown value."):
            with self.assertRaises(ValueError):
                messages.parse(struct.pack(">LB", 1, 21))

        with self.subTest("Invalid extension protocol."):
            with self.assertRaises(ValueError):
                messages.parse(struct.pack(">LBB", 2, 20, 1))

        with self.subTest("Missing b'msg_type'."):
            with self.assertRaises(ValueError):
                messages.parse(
                    _extension_message(self.ut_metadata, bencoding.encode({}))
                )

        with self.subTest("Missing b'piece'."):
            with self.assertRaises(ValueError):
                messages.parse(
                    _extension_message(
                        self.ut_metadata, bencoding.encode({b"msg_type": 0})
                    )
                )

        with self.subTest("Missing b'total_size'."):
            with self.assertRaises(ValueError):
                messages.parse(
                    _extension_message(
                        self.ut_metadata,
                        bencoding.encode({b"msg_type": 1, b"piece": 0}),
                    )
                )

        with self.subTest("Invalid b'msg_type'."):
            with self.assertRaises(ValueError):
                messages.parse(
                    _extension_message(
                        self.ut_metadata,
                        bencoding.encode({b"msg_type": 3, b"piece": 0}),
                    )
                )
