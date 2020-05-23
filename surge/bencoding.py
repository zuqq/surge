"""Recursive descent parser for bencoding."""


def _int(bs, offset):
    # Note that this doesn't follow the specification in BEP 3, because it
    # ignores leading zeros instead of raising an exception.
    end = bs.index(b"e", offset)
    return end + 1, int(bs[offset + 1 : end].decode("ascii"))


def _list(bs, offset):
    result = []
    offset += 1
    while bs[offset] != ord("e"):
        offset, rval = decode_from(bs, offset)
        result.append(rval)
    return offset + 1, result


def _dict(bs, offset):
    result = {}
    offset += 1
    while bs[offset] != ord("e"):
        offset, key = _str(bs, offset)
        offset, result[key] = decode_from(bs, offset)
    return offset + 1, result


def _str(bs, offset):
    sep_index = bs.index(b":", offset)
    end = sep_index + int(bs[offset:sep_index].decode("ascii")) + 1
    return end, bs[sep_index + 1 : end]


def decode_from(bs, offset):
    if bs[offset] == ord("i"):
        return _int(bs, offset)
    if bs[offset] == ord("l"):
        return _list(bs, offset)
    if bs[offset] == ord("d"):
        return _dict(bs, offset)
    if bs[offset] in (ord(str(i)) for i in range(10)):
        return _str(bs, offset)
    raise ValueError(bs[offset:])


def decode(bs):
    """Return the Python object corresponding to the bencoded object `bs`.

    Raises `ValueError` if `bs` is not valid BEncoding.
    """
    offset, rval = decode_from(bs, 0)
    if offset == len(bs):
        return rval
    raise ValueError(bs)


def raw_val(bs, key):
    """Return the bencoded value corresponding to `key` in the bencoded
    dictionary `bs`.

    Raises `KeyError` if `key` is not a key of `bs`.
    """
    offset = 1
    while offset < len(bs) and bs[offset] != ord("e"):
        offset, curr_key = _str(bs, offset)
        next_offset, _ = decode_from(bs, offset)
        if curr_key == key:
            return bs[offset:next_offset]
        offset = next_offset
    raise KeyError(key)


def _encode_int(n):
    return ("i" + str(n) + "e").encode("ascii")


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
    """Encode `obj` in BEncoding.

    Raises `ValueError` if `obj` is not representable in BEncoding.
    """
    if isinstance(obj, int):
        return _encode_int(obj)
    if isinstance(obj, list):
        return _encode_list(obj)
    if isinstance(obj, dict):
        return _encode_dict(obj)
    if isinstance(obj, bytes):
        return _encode_str(obj)
    raise ValueError(obj)
