"""Functions and types for interpreting `.torrent` files.

Specification: [BEP 0003], [BEP 0012]

A `.torrent` file contains tracker URLs and file metadata. For transmission,
files are concatenated and split into equally sized pieces; the pieces' SHA-1
digest is used to verify downloaded data. In order to keep `.torrent` files
small, pieces are typically relatively large; the actual transmission unit is a
block consisting of 16 KiB.

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
[BEP 0012]: http://bittorrent.org/beps/bep_0010.html
"""

from typing import List

import dataclasses
import hashlib
import pathlib

from . import bencoding


@dataclasses.dataclass(frozen=True)
class File:
    """File metadata."""

    begin: int  # Absolute offset.
    length: int
    path: pathlib.Path


@dataclasses.dataclass(frozen=True)
class Piece:
    """Piece metadata."""

    index: int
    begin: int  # Absolute offset.
    length: int
    hash: bytes  # SHA-1 digest of the piece's data.


def valid(piece, data):
    """Check whether `data`'s SHA-1 digest is equal to `piece.hash`."""
    return hashlib.sha1(data).digest() == piece.hash


@dataclasses.dataclass(frozen=True)
class Chunk:
    """The part of a `Piece` belonging to a single `File`.

    For transmission, files are concatenated and divided into pieces; a single
    piece can contain data belonging to multiple files. A `Chunk` represents a
    maximal contiguous slice of a piece belonging to a single file.
    """

    file: File
    piece: Piece
    begin: int  # Absolute offset.
    length: int


def make_chunks(pieces, files):
    """Map each element of `pieces` to a list of its `Chunk`s."""
    result = {piece: [] for piece in pieces}
    i = 0
    j = 0
    begin = 0
    while i < len(files) and j < len(pieces):
        file = files[i]
        piece = pieces[j]
        file_end = file.begin + file.length
        piece_end = piece.begin + piece.length
        if file_end <= piece_end:
            end = file_end
            i += 1
        if piece_end <= file_end:
            end = piece_end
            j += 1
        result[piece].append(Chunk(file, piece, begin, end - begin))
        begin = end
    return result


def read_chunk(chunk):
    """Read `chunk`."""
    with chunk.file.path.open("rb") as f:
        f.seek(chunk.begin - chunk.file.begin)
        return f.read(chunk.length)


def available(pieces, files):
    """Yield all valid pieces."""
    chunks = make_chunks(pieces, files)
    for piece in pieces:
        data = []
        for chunk in chunks[piece]:
            try:
                data.append(read_chunk(chunk))
            except FileNotFoundError:
                continue
        if valid(piece, b"".join(data)):
            yield piece


def write_chunk(chunk, data):
    """Write `data` to `chunk`."""
    with chunk.file.path.open("rb+") as f:
        f.seek(chunk.begin - chunk.file.begin)
        begin = chunk.begin - chunk.piece.begin
        f.write(data[begin : begin + chunk.length])


@dataclasses.dataclass(frozen=True)
class Block:
    """Block metadata."""

    piece: Piece
    begin: int
    length: int


def blocks(piece):
    """Yields `piece`'s `Block`s."""
    block_length = 2 ** 14
    for begin in range(0, piece.length, block_length):
        yield Block(piece, begin, min(block_length, piece.length - begin))


@dataclasses.dataclass
class Metadata:
    """The information contained in a `.torrent` file.

    Supports files with multiple trackers, as specified in [BEP 0012].

    [BEP 0012]: http://bittorrent.org/beps/bep_0012.html
    """

    info_hash: bytes
    announce_list: List[str]
    pieces: List[Piece]
    files: List[File]

    @classmethod
    def from_bytes(cls, raw_metadata):
        """Parse a `.torrent` file."""
        d = bencoding.decode(raw_metadata)

        announce_list = []
        if b"announce-list" in d:  # See BEP 12.
            # I'm ignoring the tiered structure because I'll be requesting peers
            # from every tracker; it's also not supported by magnet links.
            for tier in d[b"announce-list"]:
                for raw_announce in tier:
                    announce_list.append(raw_announce.decode())
        elif b"announce" in d:
            # According to BEP 12, `b"announce"` should be ignored if
            # `b"announce-list"` is present.
            announce_list.append(d[b"announce"].decode())

        info = d[b"info"]

        files = []
        if b"length" in info:
            # Single file mode.
            length = info[b"length"]
            files.append(File(0, length, pathlib.Path(info[b"name"].decode())))
        else:
            # Multiple file mode.
            length = 0  # Will be computed below.
            folder = pathlib.Path(info[b"name"].decode())
            for f in info[b"files"]:
                file = File(
                    length,
                    f[b"length"],
                    folder.joinpath(*(part.decode() for part in f[b"path"])),
                )
                files.append(file)
                length += file.length

        piece_length = info[b"piece length"]
        hashes = info[b"pieces"]
        pieces = []
        i = 0
        begin = 0
        for j in range(0, len(hashes), 20):
            end = min(length, begin + piece_length)
            pieces.append(Piece(i, begin, end - begin, hashes[j : j + 20]))
            i += 1
            begin = end

        return cls(
            hashlib.sha1(bencoding.raw_val(raw_metadata, b"info")).digest(),
            announce_list,
            pieces,
            files,
        )
