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
    length = 1
    reference = struct.pack(">LBB", 2, 5, 1 << 7)

    def test_to_bytes(self):
        self.assertEqual(
            messages.Bitfield.from_indices(self.indices, self.length).to_bytes(),
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


class _MetadataMixin:
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

    @classmethod
    def _metadata_message(cls, payload):
        n = len(payload)
        return struct.pack(f">LBB{n}s", n + 2, 20, cls.ut_metadata, payload)


class TestExtensionHandshake(unittest.TestCase, _MetadataMixin):
    @classmethod
    def setUpClass(cls):
        payload = bencoding.encode(
            {
                b"m": {b"ut_metadata": cls.ut_metadata},
                b"metadata_size": cls.metadata_size,
            }
        )
        n = len(payload)
        cls.reference = struct.pack(f">LBB{n}s", n + 2, 20, 0, payload)

    def test_to_bytes(self):
        message = messages.ExtensionHandshake(self.ut_metadata, self.metadata_size)

        self.assertEqual(message.to_bytes(), self.reference)

    def test_parse(self):
        message = messages.parse(self.reference)

        self.assertEqual(message.ut_metadata, self.ut_metadata)
        self.assertEqual(message.metadata_size, self.metadata_size)


class TestMetadataData(unittest.TestCase, _MetadataMixin):
    @classmethod
    def setUpClass(cls):
        cls.index = 0
        cls.reference = cls._metadata_message(
            b"".join(
                (
                    bencoding.encode(
                        {
                            b"msg_type": 1,
                            b"piece": cls.index,
                            b"total_size": cls.metadata_size,
                        }
                    ),
                    cls.raw_info,
                )
            )
        )

    def test_metadata_data(self):
        message = messages.MetadataData(
            self.index, self.metadata_size, self.raw_info, ut_metadata=self.ut_metadata
        )

        self.assertEqual(message.to_bytes(), self.reference)

    def test_parse(self):
        message = messages.parse(self.reference)

        self.assertEqual(message.index, self.index)
        self.assertEqual(message.total_size, self.metadata_size)
        self.assertEqual(message.data, self.raw_info)


class TestMetadataReject(unittest.TestCase, _MetadataMixin):
    @classmethod
    def setUpClass(cls):
        cls.index = 0
        cls.reference = cls._metadata_message(
            bencoding.encode({b"msg_type": 2, b"piece": cls.index})
        )

    def test_to_bytes(self):
        message = messages.MetadataReject(self.index, ut_metadata=self.ut_metadata)

        self.assertEqual(message.to_bytes(), self.reference)

    def test_parse(self):
        self.assertEqual(messages.parse(self.reference).index, self.index)


class TestMetadataRequest(unittest.TestCase, _MetadataMixin):
    @classmethod
    def setUpClass(cls):
        cls.index = 0
        cls.reference = cls._metadata_message(
            bencoding.encode({b"msg_type": 0, b"piece": cls.index})
        )

    def test_to_bytes(self):
        message = messages.MetadataRequest(self.index, ut_metadata=self.ut_metadata)

        self.assertEqual(message.to_bytes(), self.reference)

    def test_parse(self):
        self.assertEqual(messages.parse(self.reference).index, self.index)


class TestParseFailures(unittest.TestCase, _MetadataMixin):
    def test_missing_prefix(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">B", 0))

    def test_wrong_prefix(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LB", 0, 0))

    def test_unknown_identifier(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LB", 1, 21))

    def test_invalid_extension_protocol(self):
        with self.assertRaises(ValueError):
            messages.parse(struct.pack(">LBB", 2, 20, 1))

    def test_invalid_metadata_protocol(self):
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
