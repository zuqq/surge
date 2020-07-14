import struct
import unittest

from surge import bencoding
from surge import metadata
from surge import messages

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

    def test_extension_protocol(self):
        message = messages.Handshake(
            self.info_hash, self.peer_id, extension_protocol=True
        )
        _, _, reserved, _, _ = struct.unpack(self.format, message.to_bytes())

        self.assertTrue(reserved & (1 << 20))


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
            messages.Request.from_block(self.block).to_bytes(), self.reference,
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


def metadata_message(payload):
    return struct.pack(f">LBB{len(payload)}s", 1 + 1 + len(payload), 20, 3, payload)


class ExtensionProtocolTest(Example):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.metadata_size = len(cls.raw_info)

    def test_extension_handshake(self):
        with self.subTest("to_bytes"):
            message = messages.extension_handshake().to_bytes()
            _, value, extension_value, payload = struct.unpack(
                f">LBB{len(message) - 4 - 1 - 1}s", message
            )
            self.assertEqual(value, 20)
            self.assertEqual(extension_value, 0)
            d = bencoding.decode(payload)
            self.assertIn(b"m", d)
            self.assertIn(b"ut_metadata", d[b"m"])

        with self.subTest("parse"):
            payload = bencoding.encode(
                {b"m": {b"ut_metadata": 3}, b"metadata_size": self.metadata_size,}
            )
            reference = struct.pack(
                f">LBB{len(payload)}s", 1 + 1 + len(payload), 20, 0, payload
            )
            message = messages.parse(reference)
            self.assertIsInstance(message, messages.ExtensionHandshake)
            self.assertEqual(message.ut_metadata, 3)
            self.assertEqual(message.metadata_size, self.metadata_size)

    def test_metadata_data(self):
        payload = bencoding.encode(
            {b"msg_type": 1, b"piece": 0, b"total_size": self.metadata_size}
        )
        reference = metadata_message(payload + self.raw_info)

        with self.subTest("to_bytes"):
            self.assertEqual(
                messages.metadata_data(0, self.metadata_size, self.raw_info).to_bytes(),
                reference,
            )

        with self.subTest("parse"):
            message = messages.parse(reference)
            self.assertIsInstance(message, messages.MetadataData)
            self.assertEqual(message.index, 0)
            self.assertEqual(message.total_size, self.metadata_size)
            self.assertEqual(message.data, self.raw_info)

    def test_metadata_reject(self):
        reference = metadata_message(bencoding.encode({b"msg_type": 2, b"piece": 0}))

        with self.subTest("to_bytes"):
            self.assertEqual(messages.metadata_reject(0).to_bytes(), reference)

        with self.subTest("parse"):
            message = messages.parse(reference)
            self.assertIsInstance(message, messages.MetadataReject)
            self.assertEqual(message.index, 0)

    def test_metadata_request(self):
        reference = metadata_message(bencoding.encode({b"msg_type": 0, b"piece": 0}))

        with self.subTest("to_bytes"):
            self.assertEqual(messages.metadata_request(0, 3).to_bytes(), reference)

        with self.subTest("parse"):
            message = messages.parse(reference)
            self.assertIsInstance(message, messages.MetadataRequest)
            self.assertEqual(message.index, 0)


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

    def test_extension_parse(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LBB", 2, 20, 1))

    def test_metadata_parse(self):
        with self.subTest("missing b'msg_type'"):
            with self.assertRaises(ValueError):
                messages.parse(metadata_message(bencoding.encode({})))

        with self.subTest("missing b'piece'"):
            with self.assertRaises(ValueError):
                messages.parse(metadata_message(bencoding.encode({b"msg_type": 0})))

        with self.subTest("missing b'total_size'"):
            with self.assertRaises(ValueError):
                messages.parse(
                    metadata_message(bencoding.encode({b"msg_type": 1, b"piece": 0}))
                )

        with self.subTest("invalid b'msg_type'"):
            with self.assertRaises(ValueError):
                messages.parse(
                    metadata_message(bencoding.encode({b"msg_type": 3, b"piece": 0}))
                )
