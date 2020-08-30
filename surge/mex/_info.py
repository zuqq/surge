from typing import Iterable

import hashlib

from .. import bencoding


def valid(info_hash: bytes, raw_info: bytes) -> bool:
    return hashlib.sha1(raw_info).digest() == info_hash


def assemble(announce_list: Iterable[str], raw_info: bytes) -> bytes:
    # We can't just decode and re-encode, because the value associated with
    # the key `b"info"` needs to be preserved exactly.
    return b"".join(
        (
            b"d",
            b"13:announce-list",
            bencoding.encode([[url.encode() for url in announce_list]]),
            b"4:info",
            raw_info,
            b"e",
        )
    )
