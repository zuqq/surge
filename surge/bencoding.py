"""Functions for converting to and from BEncoding.

Specification: [BEP 0003]

BEncoding is a serialization format that is used to exchange dictionary-like
data with peers.

This module provides the functions `decode` and `encode`, as well as the
lower-level `decode_from` and `raw_val`.

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
"""

import io

type BEncodedValue = int | bytes | list[BEncodedValue] | dict[bytes, BEncodedValue]


def _decode_int(bs: bytes, start: int) -> tuple[int, int]:
    # More lenient than the specification: ignores leading zeros instead of
    # raising an exception.
    end = bs.index(b"e", start)
    return end + 1, int(bs[start + 1 : end].decode("ascii"))


def _decode_list(bs: bytes, start: int) -> tuple[int, list[BEncodedValue]]:
    result: list[BEncodedValue] = []
    start += 1
    while bs[start] != ord("e"):
        start, rval = decode_from(bs, start)
        result.append(rval)
    return start + 1, result


def _decode_dict(bs: bytes, start: int) -> tuple[int, dict[bytes, BEncodedValue]]:
    # More lenient than the specification: doesn't check that the dictionary
    # keys are sorted.
    result: dict[bytes, BEncodedValue] = {}
    start += 1
    while bs[start] != ord("e"):
        start, key = _decode_bytes(bs, start)
        start, result[key] = decode_from(bs, start)
    return start + 1, result


def _decode_bytes(bs: bytes, start: int) -> tuple[int, bytes]:
    sep_index = bs.index(b":", start)
    end = sep_index + int(bs[start:sep_index].decode("ascii")) + 1
    return end, bs[sep_index + 1 : end]


def decode_from(bs: bytes, start: int) -> tuple[int, BEncodedValue]:
    try:
        token = bs[start]
    except IndexError as exc:
        raise ValueError(f"Expected more input at index {start}.") from exc
    if token == ord("i"):
        return _decode_int(bs, start)
    if token == ord("l"):
        return _decode_list(bs, start)
    if token == ord("d"):
        return _decode_dict(bs, start)
    if ord("0") <= token <= ord("9"):
        return _decode_bytes(bs, start)
    raise ValueError(f"Unexpected token at index {start}.")


def decode(bs: bytes) -> BEncodedValue:
    """Return the Python object corresponding to `bs`.

    Raise `ValueError` if `bs` is not valid BEncoding.
    """
    start, rval = decode_from(bs, 0)
    if start == len(bs):
        return rval
    raise ValueError(f"Leftover bytes at index {start}.")


def raw_val(bs: bytes, key: bytes) -> bytes:
    """Return the value associated with `key` in the encoded dictionary `bs`.

    Raise `KeyError` if `key` is not a key of `bs`.
    """
    start = 1
    while start < len(bs) and bs[start] != ord("e"):
        start, curr_key = _decode_bytes(bs, start)
        next_start, _ = decode_from(bs, start)
        if curr_key == key:
            return bs[start:next_start]
        start = next_start
    raise KeyError(key)


def _encode_int(buf: io.BytesIO, n: int) -> None:
    buf.write(b"i%de" % n)


def _encode_list(buf: io.BytesIO, lst: list[BEncodedValue]) -> None:
    buf.write(b"l")
    for obj in lst:
        _encode(buf, obj)
    buf.write(b"e")


def _encode_dict(buf: io.BytesIO, d: dict[bytes, BEncodedValue]) -> None:
    buf.write(b"d")
    for key, value in sorted(d.items()):
        _encode_bytes(buf, key)
        _encode(buf, value)
    buf.write(b"e")


def _encode_bytes(buf: io.BytesIO, bs: bytes) -> None:
    buf.write(b"%d:" % len(bs))
    buf.write(bs)


def _encode(buf: io.BytesIO, obj: BEncodedValue) -> None:
    if isinstance(obj, int):
        _encode_int(buf, obj)
    elif isinstance(obj, list):
        _encode_list(buf, obj)
    elif isinstance(obj, dict):
        _encode_dict(buf, obj)
    elif isinstance(obj, bytes):
        _encode_bytes(buf, obj)
    else:
        raise TypeError(type(obj))


def encode(obj: BEncodedValue) -> bytes:
    """Encode `obj`.

    Raise `TypeError` if `obj` is not representable in BEncoding.
    """
    buf = io.BytesIO()
    _encode(buf, obj)
    return buf.getvalue()
