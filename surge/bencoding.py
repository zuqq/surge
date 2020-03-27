"""Recursive descent parser for bencoding."""


def _int(bs, offset):
    # Note that this doesn't follow the specification in BEP 3, because it
    # ignores leading zeroes instead of throwing an error.
    end = bs.index(b"e", offset)
    return end + 1, int(bs[offset + 1 : end].decode("ascii"))


def _list(bs, offset):
    result = []
    offset += 1
    while bs[offset] != ord("e"):
        offset, rval = _parse(bs, offset)
        result.append(rval)
    return offset + 1, result


def _dict(bs, offset):
    result = {}
    offset += 1
    while bs[offset] != ord("e"):
        offset, key = _str(bs, offset)
        offset, result[key] = _parse(bs, offset)
    return offset + 1, result


def _str(bs, offset):
    sep_index = bs.index(b":", offset)
    end = sep_index + int(bs[offset:sep_index].decode("ascii")) + 1
    return end, bs[sep_index + 1 : end]


def _parse(bs, offset):
    if bs[offset] == ord("i"):
        return _int(bs, offset)
    if bs[offset] == ord("l"):
        return _list(bs, offset)
    if bs[offset] == ord("d"):
        return _dict(bs, offset)
    if bs[offset] in (ord(str(i)) for i in range(10)):
        return _str(bs, offset)


def decode(bs):
    """Return the Python object corresponding to the bencoded object `bs`."""
    offset, rval = _parse(bs, 0)
    if offset == len(bs):
        return rval
    raise ValueError


def raw_val(bs, key):
    """Return the bencoded value corresponding to `key` in the bencoded
    dictionary `bs`."""
    offset = 1
    while offset < len(bs) and bs[offset] != ord("e"):
        offset, curr_key = _str(bs, offset)
        next_offset, _ = _parse(bs, offset)
        if curr_key == key:
            return bs[offset:next_offset]
        offset = next_offset
    raise KeyError
