import struct
import unittest

from surge import bencoding
from surge import messages
from surge import metadata

from ._example import Example


class HandshakeTest(Example):
    format = ">B19sQ20s20s"
    pstrlen = 19
    pstr = b"BitTorrent protocol"
    reserved = 0

    def test_to_bytes(self):
        handshake = messages.Handshake(self.info_hash, self.peer_id).to_bytes()
        pstrlen, pstr, _, info_hash, peer_id = struct.unpack(self.format, handshake)

        self.assertEqual(pstrlen, self.pstrlen)
        self.assertEqual(pstr, self.pstr)
        self.assertEqual(info_hash, self.info_hash)
        self.assertEqual(peer_id, self.peer_id)

    def test_from_bytes(self):
        message = messages.Handshake.from_bytes(
            struct.pack(
                self.format,
                self.pstrlen,
                self.pstr,
                self.reserved,
                self.info_hash,
                self.peer_id,
            )
        )

        self.assertEqual(message.info_hash, self.info_hash)
        self.assertEqual(message.peer_id, self.peer_id)


class KeepaliveTest(unittest.TestCase):
    reference = struct.pack(">L", 0)

    def test_to_bytes(self):
        self.assertEqual(messages.Keepalive().to_bytes(), self.reference)

    def test_parse(self):
        self.assertIsInstance(messages.parse(self.reference), messages.Keepalive)


class ChokeTest(unittest.TestCase):
    reference = struct.pack(">LB", 1, 0)

    def test_to_bytes(self):
        self.assertEqual(messages.Choke().to_bytes(), self.reference)

    def test_parse(self):
        self.assertIsInstance(messages.parse(self.reference), messages.Choke)


class UnchokeTest(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Unchoke().to_bytes(), struct.pack(">LB", 1, 1))


class InterestedTest(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Interested().to_bytes(), struct.pack(">LB", 1, 2))


class NotInterestedTest(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.NotInterested().to_bytes(), struct.pack(">LB", 1, 3))


class HaveTest(Example):
    reference = struct.pack(">LBL", 5, 4, 0)

    def test_to_bytes(self):
        self.assertEqual(messages.Have(0).to_bytes(), self.reference)

    def test_from_bytes(self):
        self.assertEqual(messages.Have.from_bytes(self.reference).index, 0)

    def test_piece(self):
        self.assertEqual(messages.Have(0).piece(self.pieces), self.pieces[0])


class BitfieldTest(Example):
    reference = struct.pack(">LBB", 2, 5, 1 << 7)

    def test_to_bytes(self):
        self.assertEqual(
            messages.Bitfield.from_indices({0}, len(self.pieces)).to_bytes(),
            self.reference,
        )

    def test_from_bytes(self):
        self.assertEqual(
            messages.Bitfield.from_bytes(self.reference).available(self.pieces),
            {self.pieces[0]},
        )


class RequestTest(Example):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        block, *_ = metadata.blocks(cls.pieces[0])
        cls.block = block
        cls.reference = struct.pack(
            ">LBLLL", 13, 6, block.piece.index, block.begin, block.length
        )

    def test_to_bytes(self):
        self.assertEqual(
            messages.Request.from_block(self.block).to_bytes(),
            self.reference,
        )

    def test_block(self):
        self.assertEqual(
            messages.Request.from_bytes(self.reference).block(self.pieces), self.block
        )

class BlockTest(Example):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        block, *_ = metadata.blocks(cls.pieces[0])
        cls.block = block
        cls.block_data = cls.data[0][block.begin : block.begin + block.length]
        cls.reference = struct.pack(
            ">LBLL1s", 14, 7, block.piece.index, block.begin, cls.block_data
        )

    def test_to_bytes(self):
        self.assertEqual(
            messages.Block.from_block(self.block, self.block_data).to_bytes(),
            self.reference,
        )

    def test_block(self):
        self.assertEqual(
            messages.Block.from_bytes(self.reference).block(self.pieces), self.block
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
