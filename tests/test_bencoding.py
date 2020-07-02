import unittest

from surge import bencoding


class BencodingTest(unittest.TestCase):
    # Examples from BEP 3.
    valid = [
        (b"i3e", 3),
        (b"i-3e", -3),
        (b"i0e", 0),
        (b"l4:spam4:eggse", [b"spam", b"eggs"]),
        (b"d3:cow3:moo4:spam4:eggse", {b"cow": b"moo", b"spam": b"eggs"}),
        (b"d4:spaml1:a1:bee", {b"spam": [b"a", b"b"]}),
    ]

    def test_decode(self):
        for x, y in self.valid:
            with self.subTest(x=x, y=y):
                self.assertEqual(bencoding.decode(x), y)

        for x in [b"", b"ie", b"iae", b"dde", b"2:abc"]:
            with self.subTest(x=x):
                with self.assertRaises(ValueError):
                    bencoding.decode(x)

    def test_encode(self):
        for x, y in self.valid:
            with self.subTest(x=x, y=y):
                self.assertEqual(bencoding.encode(y), x)

        for y in [None, "utf-8", 1.0, (0, 1), {0: 1}]:
            with self.subTest(y=y):
                with self.assertRaises(TypeError):
                    bencoding.encode(y)

    def test_raw_val(self):
        examples = [
            (b"d3:cow3:moo4:spam4:eggse", b"cow", b"3:moo"),
            (b"d3:cow3:moo4:spam4:eggse", b"spam", b"4:eggs"),
            (b"d4:spaml1:a1:bee", b"spam", b"l1:a1:be"),
        ]

        for x, k, v in examples:
            with self.subTest(x=x, k=k, v=v):
                self.assertEqual(bencoding.raw_val(x, k), v)

        with self.subTest("KeyError"):
            with self.assertRaises(KeyError):
                bencoding.raw_val(b"d4:spaml1:a1:bee", b"eggs")
