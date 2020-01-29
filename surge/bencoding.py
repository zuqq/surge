"""Recursive descent parser for bencoding."""


def _int(input, pos):
    # Note that this doesn't follow the specification in BEP 3, because it
    # ignores leading zeroes instead of throwing an error.
    end = input.index(b"e", pos)
    return end + 1, int(input[pos + 1 : end].decode("ascii"))


def _list(input, pos):
    result = []
    pos += 1
    while input[pos] != ord("e"):
        pos, rval = _parse(input, pos)
        result.append(rval)
    return pos + 1, result


def _dict(input, pos):
    result = {}
    pos += 1
    while input[pos] != ord("e"):
        pos, key = _str(input, pos)
        pos, result[key] = _parse(input, pos)
    return pos + 1, result


def _str(input, pos):
    sep_index = input.index(b":", pos)
    end = sep_index + int(input[pos:sep_index].decode("ascii")) + 1
    return end, input[sep_index + 1 : end]


def _parse(input, pos):
    if input[pos] == ord("i"):
        return _int(input, pos)
    if input[pos] == ord("l"):
        return _list(input, pos)
    if input[pos] == ord("d"):
        return _dict(input, pos)
    if input[pos] in (ord(str(i)) for i in range(10)):
        return _str(input, pos)


def decode(input):
    """Get the Python object corresponding to the bencoded object input."""
    pos, rval = _parse(input, 0)
    if pos == len(input):
        return rval
    raise ValueError


def raw_val(input, key):
    """Get the bencoded value corresponding to key in the bencoded dictionary input."""
    pos = 1
    while pos < len(input) and input[pos] != ord("e"):
        pos, curr_key = _str(input, pos)
        next_pos, _ = _parse(input, pos)
        if curr_key == key:
            return input[pos:next_pos]
        pos = next_pos
    raise KeyError
