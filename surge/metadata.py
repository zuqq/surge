"""Functions and types for interpreting `.torrent` files.

Specification: [BEP 0003], [BEP 0012]

A `.torrent` file contains tracker URLs and file metadata. For transmission,
files are concatenated and split into equally sized pieces; the pieces' SHA-1
digest is used to verify downloaded data. In order to keep `.torrent` files
small, pieces are typically relatively large; the actual transmission unit is a
block consisting of 16 KB.

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
[BEP 0012]: http://bittorrent.org/beps/bep_0010.html
"""

from __future__ import annotations
from typing import Any, Dict, Generator, Iterable, List, Sequence

import dataclasses
import hashlib
import os

from . import bencoding


@dataclasses.dataclass(eq=True, frozen=True)
class File:
    """File metadata."""

    begin: int
    length: int
    path: str  # TODO: Use `pathlib` instead?

    @classmethod
    def from_dict(cls, begin: int, d: Dict[bytes, Any]) -> File:
        length = d[b"length"]
        path = os.path.join(*(part.decode() for part in d[b"path"]))
        return cls(begin, length, path)


def build_file_tree(folder: str, files: Iterable[File]):
    """Create the files that don't exist and truncate the ones that do.

    Existing files need to be truncated because later writes only happen inside
    of the boundaries defined by the `File` instance.
    """
    for file in files:
        path = os.path.join(folder, file.path)
        tail, _ = os.path.split(path)
        if tail:
            os.makedirs(tail, exist_ok=True)
        with open(path, "a+b") as f:
            f.truncate(file.length)


@dataclasses.dataclass(eq=True, frozen=True)
class Piece:
    """Piece metadata."""

    index: int
    begin: int
    length: int
    hash: bytes  # SHA-1 digest of the piece's data.


def valid_piece(piece: Piece, data: bytes) -> bool:
    """Check whether `data`'s SHA-1 digest is equal to `piece.hash`."""
    return hashlib.sha1(data).digest() == piece.hash


def available_pieces(
    pieces: Sequence[Piece], folder: str, files: Sequence[File]
) -> Generator[Piece, None, None]:
    """Yield all pieces whose SHA-1 digest is correct."""
    chunks = piece_to_chunks(pieces, files)
    for piece in pieces:
        data = []
        for chunk in chunks[piece]:
            path = os.path.join(folder, chunk.file.path)
            try:
                with open(path, "rb") as f:
                    f.seek(chunk.begin - chunk.file.begin)
                    data.append(f.read(chunk.length))
            except FileNotFoundError:
                continue
        if valid_piece(piece, b"".join(data)):
            yield piece


@dataclasses.dataclass(eq=True, frozen=True)
class Chunk:
    """The part of a `Piece` belonging to a single `File`."""

    file: File
    piece: Piece
    begin: int
    length: int


def piece_to_chunks(
    pieces: Sequence[Piece], files: Sequence[File]
) -> Dict[Piece, List[Chunk]]:
    """Map pieces to their chunks."""
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


def write_chunk(folder: str, chunk: Chunk, data: bytes):
    """Write `chunk` to the file system."""
    path = os.path.join(folder, chunk.file.path)
    with open(path, "rb+") as f:
        f.seek(chunk.begin - chunk.file.begin)
        begin = chunk.begin - chunk.piece.begin
        f.write(data[begin : begin + chunk.length])


@dataclasses.dataclass(eq=True, frozen=True)
class Block:
    """Block metadata."""

    piece: Piece
    begin: int
    length: int


def blocks(piece: Piece) -> Generator[Block, None, None]:
    """Yields `piece`'s `Block`s."""
    block_size = 2 ** 14
    for begin in range(0, piece.length, block_size):
        yield Block(piece, begin, min(block_size, piece.length - begin))


@dataclasses.dataclass
class Metadata:
    """The information contained in a `.torrent` file.

    Supports files with multiple trackers, as specified in [BEP 0012].

    [BEP 0012]: http://bittorrent.org/beps/bep_0012.html
    """

    announce_list: List[str]
    length: int
    piece_length: int
    pieces: List[Piece]
    folder: str
    files: List[File]

    @classmethod
    def from_bytes(cls, raw_meta: bytes) -> Metadata:
        """Parse a `.torrent` file."""
        d = bencoding.decode(raw_meta)

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
            folder = ""
            files.append(File(0, length, info[b"name"].decode()))
        else:
            # Multiple file mode.
            length = 0  # Will be computed below.
            folder = info[b"name"].decode()
            for f in info[b"files"]:
                file = File.from_dict(length, f)
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

        return cls(announce_list, length, piece_length, pieces, folder, files)
