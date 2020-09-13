"""Functions for converting to and from BEncoding.

BEncoding is defined in the [BitTorrent specification][0].

[0]: http://bittorrent.org/beps/bep_0003.html
"""


def _int(bs, start):
    # Note that this doesn't follow the specification in BEP 3, because it
    # ignores leading zeros instead of raising an exception.
    end = bs.index(b"e", start)
    return end + 1, int(bs[start + 1 : end].decode("ascii"))


def _list(bs, start):
    result = []
    start += 1
    while bs[start] != ord("e"):
        start, rval = decode_from(bs, start)
        result.append(rval)
    return start + 1, result


def _dict(bs, start):
    # TODO: The specification only allows dictionaries with sorted keys!
    result = {}
    start += 1
    while bs[start] != ord("e"):
        start, key = _str(bs, start)
        start, result[key] = decode_from(bs, start)
    return start + 1, result


def _str(bs, start):
    sep_index = bs.index(b":", start)
    end = sep_index + int(bs[start:sep_index].decode("ascii")) + 1
    return end, bs[sep_index + 1 : end]


def decode_from(bs, start):
    try:
        token = bs[start]
    # These are the exceptions that should be raised by `__getitem__`.
    except (TypeError, IndexError, KeyError) as exc:
        raise ValueError(bs[start:]) from exc
    if token == ord("i"):
        return _int(bs, start)
    if token == ord("l"):
        return _list(bs, start)
    if token == ord("d"):
        return _dict(bs, start)
    if ord("0") <= token <= ord("9"):
        return _str(bs, start)
    raise ValueError(bs[start:])


def decode(bs):
    """Return the Python object corresponding to the encoded value `bs`.

    Raise `ValueError` if `bs` is not valid BEncoding.
    """
    start, rval = decode_from(bs, 0)
    if start == len(bs):
        return rval
    raise ValueError("Not enough bytes.")


def raw_val(bs, key):
    """Return the value associated with `key` in the encoded dictionary `bs`.

    Raise `KeyError` if `key` is not a key of `bs`.
    """
    start = 1
    while start < len(bs) and bs[start] != ord("e"):
        start, curr_key = _str(bs, start)
        next_start, _ = decode_from(bs, start)
        if curr_key == key:
            return bs[start:next_start]
        start = next_start
    raise KeyError(key)


def _encode_int(n):
    return b"i" + str(n).encode("ascii") + b"e"


def _encode_list(l):
    result = [b"l"]
    for obj in l:
        result.append(encode(obj))
    result.append(b"e")
    return b"".join(result)


def _encode_dict(d):
    result = [b"d"]
    for key, value in d.items():
        result.append(_encode_str(key))
        result.append(encode(value))
    result.append(b"e")
    return b"".join(result)


def _encode_str(bs):
    return str(len(bs)).encode("ascii") + b":" + bs


def encode(obj):
    """Return the encoded value corresponding to `obj`.

    Raise `TypeError` if `obj` is not representable in BEncoding.
    """
    if isinstance(obj, int):
        return _encode_int(obj)
    if isinstance(obj, list):
        return _encode_list(obj)
    if isinstance(obj, dict):
        return _encode_dict(obj)
    if isinstance(obj, bytes):
        return _encode_str(obj)
    raise TypeError(obj)
