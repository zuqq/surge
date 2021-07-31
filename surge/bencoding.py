"""Functions for converting to and from BEncoding.

Specification: [BEP 0003]

BEncoding is a serialization format that is used to exchange dictionary-like
data with peers.

This module provides the functions `decode` and `encode`, as well as the
lower-level `decode_from` and `raw_val`.

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
"""

import io


def _decode_int(bs, start):
    # More lenient than the specification: ignores leading zeros instead of
    # raising an exception.
    end = bs.index(b"e", start)
    return end + 1, int(bs[start + 1 : end].decode("ascii"))


def _decode_list(bs, start):
    result = []
    start += 1
    while bs[start] != ord("e"):
        start, rval = decode_from(bs, start)
        result.append(rval)
    return start + 1, result


def _decode_dict(bs, start):
    # More lenient than the specification: doesn't check that the dictionary
    # keys are sorted.
    result = {}
    start += 1
    while bs[start] != ord("e"):
        start, key = _decode_bytes(bs, start)
        start, result[key] = decode_from(bs, start)
    return start + 1, result


def _decode_bytes(bs, start):
    sep_index = bs.index(b":", start)
    end = sep_index + int(bs[start:sep_index].decode("ascii")) + 1
    return end, bs[sep_index + 1 : end]


def decode_from(bs, start):
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


def decode(bs):
    """Return the Python object corresponding to `bs`.

    Raise `ValueError` if `bs` is not valid BEncoding.
    """
    start, rval = decode_from(bs, 0)
    if start == len(bs):
        return rval
    raise ValueError(f"Leftover bytes at index {start}.")


def raw_val(bs, key):
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


def _encode_int(n, buf):
    buf.write(b"i%de" % n)


def _encode_list(l, buf):  # noqa: E741
    buf.write(b"l")
    for obj in l:
        _encode(obj, buf)
    buf.write(b"e")


def _encode_dict(d, buf):
    buf.write(b"d")
    for key, value in sorted(d.items()):
        _encode_bytes(key, buf)
        _encode(value, buf)
    buf.write(b"e")


def _encode_bytes(bs, buf):
    buf.write(b"%d:" % len(bs))
    buf.write(bs)


def _encode(obj, buf):
    if isinstance(obj, int):
        _encode_int(obj, buf)
    elif isinstance(obj, list):
        _encode_list(obj, buf)
    elif isinstance(obj, dict):
        _encode_dict(obj, buf)
    elif isinstance(obj, bytes):
        _encode_bytes(obj, buf)
    else:
        raise TypeError(type(obj))


def encode(obj):
    """Encode `obj`.

    Raise `TypeError` if `obj` is not representable in BEncoding.
    """
    buf = io.BytesIO()
    _encode(obj, buf)
    return buf.getvalue()
