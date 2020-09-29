import struct
import unittest

from surge import _metadata
from surge import bencoding
from surge import messages

from ._example import Example


class TestHandshake(Example):
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
        message = messages.parse_handshake(
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


class TestKeepalive(unittest.TestCase):
    reference = struct.pack(">L", 0)

    def test_to_bytes(self):
        self.assertEqual(messages.Keepalive().to_bytes(), self.reference)

    def test_parse(self):
        self.assertIsInstance(messages.parse(self.reference), messages.Keepalive)


class TestChoke(unittest.TestCase):
    reference = struct.pack(">LB", 1, 0)

    def test_to_bytes(self):
        self.assertEqual(messages.Choke().to_bytes(), self.reference)

    def test_parse(self):
        self.assertIsInstance(messages.parse(self.reference), messages.Choke)


class TestUnchoke(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Unchoke().to_bytes(), struct.pack(">LB", 1, 1))


class TestInterested(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Interested().to_bytes(), struct.pack(">LB", 1, 2))


class TestNotInterested(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.NotInterested().to_bytes(), struct.pack(">LB", 1, 3))


class TestHave(Example):
    reference = struct.pack(">LBL", 5, 4, 0)

    def test_to_bytes(self):
        self.assertEqual(messages.Have(0).to_bytes(), self.reference)

    def test_parse(self):
        self.assertEqual(messages.parse(self.reference).index, 0)

    def test_piece(self):
        self.assertEqual(messages.Have(0).piece(self.pieces), self.pieces[0])


class TestBitfield(Example):
    reference = struct.pack(">LBB", 2, 5, 1 << 7)

    def test_to_bytes(self):
        self.assertEqual(
            messages.Bitfield.from_indices({0}, len(self.pieces)).to_bytes(),
            self.reference,
        )

    def test_parse(self):
        self.assertEqual(
            messages.parse(self.reference).available(self.pieces), {self.pieces[0]},
        )


class TestRequest(Example):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        block, *_ = _metadata.blocks(cls.pieces[0])
        cls.block = block
        cls.reference = struct.pack(
            ">LBLLL", 13, 6, block.piece.index, block.begin, block.length
        )

    def test_to_bytes(self):
        self.assertEqual(
            messages.Request.from_block(self.block).to_bytes(), self.reference,
        )

    def test_block(self):
        self.assertEqual(messages.parse(self.reference).block(self.pieces), self.block)


class TestBlock(Example):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        block, *_ = _metadata.blocks(cls.pieces[0])
        cls.block = block
        cls.block_data = cls.data[0][block.begin : block.begin + block.length]
        n = len(cls.block_data)
        cls.reference = struct.pack(
            f">LBLL{n}s", n + 9, 7, block.piece.index, block.begin, cls.block_data
        )

    def test_to_bytes(self):
        self.assertEqual(
            messages.Block.from_block(self.block, self.block_data).to_bytes(),
            self.reference,
        )

    def test_block(self):
        self.assertEqual(messages.parse(self.reference).block(self.pieces), self.block)


class ProtocolExtensionTest(Example):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.ut_metadata = 3
        cls.metadata_size = len(cls.raw_info)

    def _metadata_message(self, payload):
        n = len(payload)
        return struct.pack(f">LBB{n}s", n + 2, 20, self.ut_metadata, payload)

    def test_extension_handshake(self):
        with self.subTest("to_bytes"):
            message = messages.ExtensionHandshake(
                self.ut_metadata, self.metadata_size
            ).to_bytes()
            _, value, extension_value, payload = struct.unpack(
                f">LBB{len(message) - 6}s", message
            )
            self.assertEqual(value, 20)
            self.assertEqual(extension_value, 0)
            d = bencoding.decode(payload)
            self.assertEqual(d[b"m"][b"ut_metadata"], self.ut_metadata)
            self.assertEqual(d[b"metadata_size"], self.metadata_size)

        with self.subTest("parse"):
            payload = bencoding.encode(
                {
                    b"m": {b"ut_metadata": self.ut_metadata},
                    b"metadata_size": self.metadata_size,
                }
            )
            n = len(payload)
            reference = struct.pack(f">LBB{n}s", n + 2, 20, 0, payload)
            message = messages.parse(reference)
            self.assertIsInstance(message, messages.ExtensionHandshake)
            self.assertEqual(message.ut_metadata, self.ut_metadata)
            self.assertEqual(message.metadata_size, self.metadata_size)

    def test_metadata_data(self):
        index = 0
        payload = bencoding.encode(
            {b"msg_type": 1, b"piece": index, b"total_size": self.metadata_size}
        )
        reference = self._metadata_message(payload + self.raw_info)

        with self.subTest("to_bytes"):
            self.assertEqual(
                messages.MetadataData(
                    index, self.metadata_size, self.raw_info, self.ut_metadata
                ).to_bytes(),
                reference,
            )

        with self.subTest("parse"):
            message = messages.parse(reference)
            self.assertIsInstance(message, messages.MetadataData)
            self.assertEqual(message.index, index)
            self.assertEqual(message.total_size, self.metadata_size)
            self.assertEqual(message.data, self.raw_info)

    def test_metadata_reject(self):
        index = 0
        reference = self._metadata_message(
            bencoding.encode({b"msg_type": 2, b"piece": index})
        )

        with self.subTest("to_bytes"):
            self.assertEqual(
                messages.MetadataReject(index, self.ut_metadata).to_bytes(), reference
            )

        with self.subTest("parse"):
            message = messages.parse(reference)
            self.assertIsInstance(message, messages.MetadataReject)
            self.assertEqual(message.index, index)

    def test_metadata_request(self):
        index = 0
        reference = self._metadata_message(
            bencoding.encode({b"msg_type": 0, b"piece": index})
        )

        with self.subTest("to_bytes"):
            self.assertEqual(
                messages.MetadataRequest(index, self.ut_metadata).to_bytes(), reference
            )

        with self.subTest("parse"):
            message = messages.parse(reference)
            self.assertIsInstance(message, messages.MetadataRequest)
            self.assertEqual(message.index, index)

    def test_extension_parse(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LBB", 2, 20, 1))

    def test_metadata_parse(self):
        with self.subTest("missing b'msg_type'"):
            with self.assertRaises(ValueError):
                messages.parse(self._metadata_message(bencoding.encode({})))

        with self.subTest("missing b'piece'"):
            with self.assertRaises(ValueError):
                messages.parse(
                    self._metadata_message(bencoding.encode({b"msg_type": 0}))
                )

        with self.subTest("missing b'total_size'"):
            with self.assertRaises(ValueError):
                messages.parse(
                    self._metadata_message(
                        bencoding.encode({b"msg_type": 1, b"piece": 0})
                    )
                )

        with self.subTest("invalid b'msg_type'"):
            with self.assertRaises(ValueError):
                messages.parse(
                    self._metadata_message(
                        bencoding.encode({b"msg_type": 3, b"piece": 0})
                    )
                )


class TestParse(unittest.TestCase):
    def test_missing_prefix(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">B", 0))

    def test_wrong_prefix(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LB", 0, 0))

    def test_unknown_identifier(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LB", 1, 21))
