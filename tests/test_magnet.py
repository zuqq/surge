import unittest

from surge import magnet


class ParseTest(unittest.TestCase):
    def test_parse(self):
        with self.subTest("valid"):
            self.assertEqual(
                magnet.parse(
                    "magnet:?xt=urn:btih:86d4c80024a469be4c50bc5a102cf71780310074"
                    "&dn=debian-10.2.0-amd64-netinst.iso"
                    "&tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce"
                ),
                (
                    b"\x86\xd4\xc8\x00$\xa4i\xbeLP\xbcZ\x10,\xf7\x17\x801\x00t",
                    ["http://bttracker.debian.org:6969/announce"],
                ),
            )

        invalid = [
            "http://www.example.com",
            "magnet:?tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce",
            "magnet:?xt=aaa",
            "magnet:?xt=urn:btih:0",
            "magnet:?xt=urn:btih:86d4",
        ]

        for s in invalid:
            with self.subTest(s=s):
                with self.assertRaises(ValueError):
                    magnet.parse(s)
