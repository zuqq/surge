import pytest

from surge import bencoding


def test_decode():
    """Compare with the examples from BEP 3."""
    assert bencoding.decode(b"i3e") == 3
    assert bencoding.decode(b"i-3e") == -3
    assert bencoding.decode(b"i0e") == 0
    assert bencoding.decode(b"l4:spam4:eggse") == [b"spam", b"eggs"]
    assert bencoding.decode(b"d3:cow3:moo4:spam4:eggse") == {
        b"cow": b"moo",
        b"spam": b"eggs",
    }
    assert bencoding.decode(b"d4:spaml1:a1:bee") == {b"spam": [b"a", b"b"]}

def test_raw_val():
    assert bencoding.raw_val(b"d3:cow3:moo4:spam4:eggse", b"cow") == b"3:moo"
    assert bencoding.raw_val(b"d3:cow3:moo4:spam4:eggse", b"spam") == b"4:eggs"
    assert bencoding.raw_val(b"d4:spaml1:a1:bee", b"spam") == b"l1:a1:be"
    with pytest.raises(KeyError):
        bencoding.raw_val(b"d4:spaml1:a1:bee", b"eggs")
