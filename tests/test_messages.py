import unittest

from surge import messages


class TestMessages(unittest.TestCase):
    handshake_reference = b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x10\x00\x00\xc8\x06\x12\xfd(\xfb\xfa3g\xf2\xe9\x0c5\xd9\x9f\x93\xa1\xd24O\xad6n\x84\xb3a\xa4\xc1\xa1\xde\xd4H\x01J\xc0]\x1b\x88\x92I"
    handshake = messages.Handshake(
        messages.EXTENSION_PROTOCOL_BIT,
        b"\xc8\x06\x12\xfd(\xfb\xfa3g\xf2\xe9\x0c5\xd9\x9f\x93\xa1\xd24O",
        b"\xad6n\x84\xb3a\xa4\xc1\xa1\xde\xd4H\x01J\xc0]\x1b\x88\x92I",
    )
    bitfield_reference = b"\x00\x00\x00\x02\x05\x80"
    bitfield = messages.Bitfield.from_indices({0}, 1)
    valid = (
        (b"\x00\x00\x00\x00", messages.Keepalive()),
        (b"\x00\x00\x00\x01\x00", messages.Choke()),
        (b"\x00\x00\x00\x01\x01", messages.Unchoke()),
        (b"\x00\x00\x00\x01\x02", messages.Interested()),
        (b"\x00\x00\x00\x01\x03", messages.NotInterested()),
        (b"\x00\x00\x00\x05\x04\x00\x00\x00\x00", messages.Have(0)),
        (
            b"\x00\x00\x00\r\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02",
            messages.Request(0, 0, 2),
        ),
        (
            b"\x00\x00\x00\x0b\x07\x00\x00\x00\x00\x00\x00\x00\x00a\n",
            messages.Block(0, 0, b"a\n"),
        ),
        (
            b"\x00\x00\x00\r\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02",
            messages.Cancel(0, 0, 2),
        ),
        (
            b"\x00\x00\x00.\x14\x00d1:md11:ut_metadatai3ee13:metadata_sizei76ee",
            messages.ExtensionHandshake(3, 76),
        ),
        (
            b"\x00\x00\x00x\x14\x03d8:msg_typei1e5:piecei0e10:total_sizei76eed6:lengthi2e4:name1:a12:piece_lengthi262144e6:pieces20:?xhP\xe3\x87U\x0f\xda\xb86\xed~m\xc8\x81\xde#\x00\x1be",
            messages.MetadataData(
                0,
                76,
                b"d6:lengthi2e4:name1:a12:piece_lengthi262144e6:pieces20:?xhP\xe3\x87U\x0f\xda\xb86\xed~m\xc8\x81\xde#\x00\x1be",
            ),
        ),
        (
            b"\x00\x00\x00\x1b\x14\x03d8:msg_typei2e5:piecei0ee",
            messages.MetadataReject(0),
        ),
        (
            b"\x00\x00\x00\x1b\x14\x03d8:msg_typei0e5:piecei0ee",
            messages.MetadataRequest(0),
        ),
    )

    def test_to_bytes(self):
        with self.subTest(self.handshake):
            self.assertEqual(self.handshake.to_bytes(), self.handshake_reference)

        with self.subTest(self.bitfield):
            self.assertEqual(self.bitfield.to_bytes(), self.bitfield_reference)

        for x, y in self.valid:
            with self.subTest(y):
                self.assertEqual(y.to_bytes(), x)

    def test_to_indices(self):
        self.assertEqual(self.bitfield.to_indices(), {0})

    def test_parse_handshake(self):
        self.assertEqual(
            messages.parse_handshake(self.handshake_reference), self.handshake
        )

    def test_parse(self):
        with self.subTest(self.bitfield_reference):
            self.assertEqual(messages.parse(self.bitfield_reference), self.bitfield)

        for x, y in self.valid:
            with self.subTest(x):
                self.assertEqual(messages.parse(x), y)

        with self.subTest("Missing prefix."):
            with self.assertRaises(ValueError):
                messages.parse(b"\x00")

        with self.subTest("Wrong prefix."):
            with self.assertRaises(ValueError):
                messages.parse(b"\x00\x00\x00\x00\x00")

        with self.subTest("Unknown value."):
            with self.assertRaises(ValueError):
                messages.parse(b"\x00\x00\x00\x01\x15")

        with self.subTest("Invalid extension protocol."):
            with self.assertRaises(ValueError):
                messages.parse(b"\x00\x00\x00\x02\x14\x01")

        with self.subTest("Missing b'msg_type'."):
            with self.assertRaises(ValueError):
                messages.parse(b"\x00\x00\x00\x04\x14\x03de")

        with self.subTest("Missing b'piece'."):
            with self.assertRaises(ValueError):
                messages.parse(b"\x00\x00\x00\x11\x14\x03d8:msg_typei0ee")

        with self.subTest("Missing b'total_size'."):
            with self.assertRaises(ValueError):
                messages.parse(b"\x00\x00\x00\x1b\x14\x03d8:msg_typei1e5:piecei0ee")

        with self.subTest("Invalid b'msg_type'."):
            with self.assertRaises(ValueError):
                messages.parse(b"\x00\x00\x00\x1b\x14\x03d8:msg_typei3e5:piecei0ee")
