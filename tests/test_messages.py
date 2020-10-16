import hashlib
import struct
import unittest

from surge import _metadata
from surge import bencoding
from surge import messages


class TestHandshake(unittest.TestCase):
    reserved = 1 << 20
    info_hash = b"O\xd2\xd3Y\x8a\x11\x01\xa1U\xdd\x86|\x91\x04\xfc\xd2\xd9\xe4$+"
    peer_id = b"\xa7\x88\x06\x8b\xeb6i~=//\x1e\xc8\x1d\xbb\x12\x023\xa58"
    reference = struct.pack(
        ">B19sQ20s20s", 19, b"BitTorrent protocol", reserved, info_hash, peer_id
    )

    def test_to_bytes(self):
        self.assertEqual(
            messages.Handshake(self.reserved, self.info_hash, self.peer_id).to_bytes(),
            self.reference,
        )

    def test_parse_handshake(self):
        message = messages.parse_handshake(self.reference)

        self.assertEqual(message.reserved, self.reserved)
        self.assertEqual(message.info_hash, self.info_hash)
        self.assertEqual(message.peer_id, self.peer_id)


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


class TestUnchoke(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Unchoke().to_bytes(), struct.pack(">LB", 1, 1))


class TestInterested(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Interested().to_bytes(), struct.pack(">LB", 1, 2))


class TestNotInterested(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.NotInterested().to_bytes(), struct.pack(">LB", 1, 3))


class TestHave(unittest.TestCase):
    def test_to_bytes(self):
        self.assertEqual(messages.Have(0).to_bytes(), struct.pack(">LBL", 5, 4, 0))


class TestBitfield(unittest.TestCase):
    indices = {0}
    total = 1
    reference = struct.pack(">LBB", 2, 5, 1 << 7)

    def test_to_bytes(self):
        self.assertEqual(
            messages.Bitfield.from_indices(self.indices, self.total).to_bytes(),
            self.reference,
        )

    def test_parse(self):
        self.assertEqual(messages.parse(self.reference).to_indices(), self.indices)


class _BlockMixin:
    data = b"a"
    piece = _metadata.Piece(0, 0, 1, hashlib.sha1(data).digest())
    block = _metadata.Block(piece, 0, 1)


class TestRequest(unittest.TestCase, _BlockMixin):
    @classmethod
    def setUpClass(cls):
        cls.reference = struct.pack(
            ">LBLLL", 13, 6, cls.block.piece.index, cls.block.begin, cls.block.length
        )

    def test_to_bytes(self):
        self.assertEqual(
            messages.Request.from_block(self.block).to_bytes(), self.reference
        )

    def test_parse(self):
        self.assertEqual(messages.parse(self.reference).index, self.block.piece.index)


class TestBlock(unittest.TestCase, _BlockMixin):
    @classmethod
    def setUpClass(cls):
        n = len(cls.data)
        cls.reference = struct.pack(
            f">LBLL{n}s", n + 9, 7, cls.block.piece.index, cls.block.begin, cls.data
        )

    def test_to_bytes(self):
        self.assertEqual(
            messages.Block.from_block(self.block, self.data).to_bytes(), self.reference
        )

    def test_parse(self):
        self.assertEqual(messages.parse(self.reference).index, self.block.piece.index)


class ProtocolExtensionTest(unittest.TestCase):
    raw_info = bencoding.encode(
        {
            b"length": 1,
            b"name": b"a",
            b"piece length": 2 ** 18,
            b"pieces": b"\xa0\xf1I\n \xd0!\x1c\x99{D\xbc5~\x19r\xde\xab\x8a\xe3",
        }
    )
    ut_metadata = 3
    metadata_size = len(raw_info)

    def test_extension_handshake(self):
        payload = bencoding.encode(
            {
                b"m": {b"ut_metadata": self.ut_metadata},
                b"metadata_size": self.metadata_size,
            }
        )
        n = len(payload)
        reference = struct.pack(f">LBB{n}s", n + 2, 20, 0, payload)

        with self.subTest("to_bytes"):
            self.assertEqual(
                messages.ExtensionHandshake(
                    self.ut_metadata, self.metadata_size
                ).to_bytes(),
                reference,
            )

        with self.subTest("parse"):
            message = messages.parse(reference)

            self.assertIsInstance(message, messages.ExtensionHandshake)
            self.assertEqual(message.ut_metadata, self.ut_metadata)
            self.assertEqual(message.metadata_size, self.metadata_size)

    def _metadata_message(self, payload):
        n = len(payload)
        return struct.pack(f">LBB{n}s", n + 2, 20, self.ut_metadata, payload)

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
