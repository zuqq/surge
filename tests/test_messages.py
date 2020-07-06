import hashlib
import struct
import unittest

from surge import bencoding
from surge import messages
from surge import metadata


class ExampleMixin(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # A simple example with a single file "a" containing the byte b"s",
        # split into a single piece.
        info = {
            b"name": b"a",
            b"piece length": 2 ** 18,
            b"length": 1,
            b"pieces": b"\xa0\xf1I\n \xd0!\x1c\x99{D\xbc5~\x19r\xde\xab\x8a\xe3",
        }
        cls.info_hash = hashlib.sha1(bencoding.encode(info)).digest()
        cls.peer_id = b"\x88\x07 \x7f\x00d\xedr J\x13w~.\xb2_P\xf3\xf82"
        cls.piece = metadata.Piece(
            0, 0, 1, b"\xa0\xf1I\n \xd0!\x1c\x99{D\xbc5~\x19r\xde\xab\x8a\xe3"
        )
        cls.data = b"a"


class HandshakeTest(ExampleMixin):
    def test_to_bytes(self):
        result = messages.Handshake(self.info_hash, self.peer_id).to_bytes()

        self.assertEqual(result[0], 19)
        self.assertEqual(result[1:20], b"BitTorrent protocol")
        self.assertEqual(result[28:48], self.info_hash)
        self.assertEqual(result[48:68], self.peer_id)

    def test_from_bytes(self):
        handshake = messages.Handshake.from_bytes(
            struct.pack(
                ">B19sQ20s20s",
                19,
                b"BitTorrent protocol",
                0,
                self.info_hash,
                self.peer_id,
            )
        )

        self.assertEqual(handshake.info_hash, self.info_hash)
        self.assertEqual(handshake.peer_id, self.peer_id)


class KeepaliveTest(unittest.TestCase):
    message = struct.pack(">L", 0)

    def test_to_bytes(self):
        self.assertEqual(messages.Keepalive().to_bytes(), self.message)

    def test_parse(self):
        self.assertIsInstance(messages.parse(self.message), messages.Keepalive)


class ChokeTest(unittest.TestCase):
    message = struct.pack(">LB", 1, 0)

    def test_to_bytes(self):
        self.assertEqual(messages.Choke().to_bytes(), self.message)

    def test_parse(self):
        self.assertIsInstance(messages.parse(self.message), messages.Choke)


class UnchokeTest(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Unchoke().to_bytes(), struct.pack(">LB", 1, 1))


class InterestedTest(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Interested().to_bytes(), struct.pack(">LB", 1, 2))


class NotInterestedTest(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.NotInterested().to_bytes(), struct.pack(">LB", 1, 3))


class HaveTest(ExampleMixin):
    def test_to_bytes(self):
        self.assertEqual(messages.Have(0).to_bytes(), struct.pack(">LBL", 5, 4, 0))

    def test_from_bytes(self):
        message = struct.pack(">LBL", 5, 4, 0)
        self.assertEqual(messages.Have.from_bytes(message).index, 0)

    def test_piece(self):
        self.assertEqual(messages.Have(0).piece([self.piece]), self.piece)


class BitfieldTest(ExampleMixin):
    payload = struct.pack(">B", 1 << 7)
    message = struct.pack(">LB1s", 2, 5, payload)

    def test_to_bytes(self):
        self.assertEqual(messages.Bitfield(self.payload).to_bytes(), self.message)

    def test_from_bytes(self):
        self.assertEqual(
            messages.Bitfield.from_bytes(self.message).payload, self.payload
        )

    def test_available(self):
        self.assertEqual(
            messages.Bitfield(self.payload).available([self.piece]), {self.piece}
        )


class RequestTest(ExampleMixin):
    def test_to_bytes(self):
        (block,) = metadata.blocks(self.piece)

        self.assertEqual(
            messages.Request.from_block(block).to_bytes(),
            struct.pack(">LBLLL", 13, 6, block.piece.index, block.begin, block.length),
        )


class BlockTest(ExampleMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        (block,) = metadata.blocks(cls.piece)
        cls.block = block
        cls.message = struct.pack(
            ">LBLL1s", 14, 7, block.piece.index, block.begin, cls.data
        )

    def test_to_bytes(self):
        self.assertEqual(
            messages.Block.from_block(self.block, self.data).to_bytes(), self.message
        )

    def test_block(self):
        self.assertEqual(
            messages.Block.from_bytes(self.message).block([self.piece]), self.block
        )


class ParseTest(unittest.TestCase):
    def test_missing_prefix(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">B", 0))

    def test_wrong_prefix(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LB", 0, 0))

    def test_unknown_identifier(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LB", 1, 21))
