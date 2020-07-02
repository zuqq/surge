import pytest

from surge import bencoding


# Examples from BEP 3.
valid = [
    (b"i3e", 3),
    (b"i-3e", -3),
    (b"i0e", 0),
    (b"l4:spam4:eggse", [b"spam", b"eggs"]),
    (b"d3:cow3:moo4:spam4:eggse", {b"cow": b"moo", b"spam": b"eggs"}),
    (b"d4:spaml1:a1:bee", {b"spam": [b"a", b"b"]}),
]


def test_decode():
    for x, y in valid:
        assert bencoding.decode(x) == y

    for x in [b"", b"ie", b"iae", b"dde", b"2:abc"]:
        with pytest.raises(ValueError):
            bencoding.decode(x)


def test_encode():
    for x, y in valid:
        assert bencoding.encode(y) == x

    for y in [None, "utf-8", 1.0, (0, 1), {0: 1}]:
        with pytest.raises(TypeError):
            bencoding.encode(y)


def test_raw_val():
    examples = [
        (b"d3:cow3:moo4:spam4:eggse", b"cow", b"3:moo"),
        (b"d3:cow3:moo4:spam4:eggse", b"spam", b"4:eggs"),
        (b"d4:spaml1:a1:bee", b"spam", b"l1:a1:be"),
    ]

    for x, k, v in examples:
        assert bencoding.raw_val(x, k) == v

    with pytest.raises(KeyError):
        bencoding.raw_val(b"d4:spaml1:a1:bee", b"eggs")
