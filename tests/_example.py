import hashlib
import unittest

from surge import _metadata
from surge import bencoding


class Example(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # A simple example with a single file "a" containing the byte b"s",
        # split into a single piece.
        cls.raw_info = bencoding.encode(
            {
                b"name": b"a",
                b"piece length": 2 ** 18,
                b"length": 1,
                b"pieces": b"\xa0\xf1I\n \xd0!\x1c\x99{D\xbc5~\x19r\xde\xab\x8a\xe3",
            }
        )
        cls.info_hash = hashlib.sha1(cls.raw_info).digest()
        cls.peer_id = b"\x88\x07 \x7f\x00d\xedr J\x13w~.\xb2_P\xf3\xf82"
        cls.pieces = [
            _metadata.Piece(
                0, 0, 1, b"\xa0\xf1I\n \xd0!\x1c\x99{D\xbc5~\x19r\xde\xab\x8a\xe3"
            )
        ]
        cls.data = [b"s"]
