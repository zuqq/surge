import pytest

from surge import magnet


def test_parse():
    assert magnet.parse(
        "magnet:?xt=urn:btih:86d4c80024a469be4c50bc5a102cf71780310074"
        "&dn=debian-10.2.0-amd64-netinst.iso"
        "&tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce"
    ) == (
        b"\x86\xd4\xc8\x00$\xa4i\xbeLP\xbcZ\x10,\xf7\x17\x801\x00t",
        ["http://bttracker.debian.org:6969/announce"],
    )
    with pytest.raises(ValueError):
        magnet.parse("http://www.example.com")
    with pytest.raises(ValueError):
        magnet.parse("magnet:?tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce")
    with pytest.raises(ValueError):
        magnet.parse("magnet:?xt=aaa")
    with pytest.raises(ValueError):
        magnet.parse("magnet:?xt=urn:btih:0")
    with pytest.raises(ValueError):
        magnet.parse("magnet:?xt=urn:btih:86d4")
