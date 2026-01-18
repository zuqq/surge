"""Functions and types for interpreting `.torrent` files.

Specification: [BEP 0003], [BEP 0012]

A `.torrent` file contains tracker URLs and file metadata; the method
`Metadata.from_bytes` parses it into the structured representation that is used
in the rest of the program.

[BEP 0003]: http://bittorrent.org/beps/bep_0003.html
[BEP 0012]: http://bittorrent.org/beps/bep_0012.html
"""

import dataclasses
import hashlib
import pathlib
from collections.abc import Generator, Sequence
from typing import Any, Self

from . import bencoding


@dataclasses.dataclass(frozen=True)
class File:
    """File metadata."""

    begin: int  # Absolute offset.
    length: int
    path: pathlib.Path  # Relative path to the file.


@dataclasses.dataclass(frozen=True)
class Piece:
    """Piece metadata."""

    index: int
    begin: int  # Absolute offset.
    length: int
    hash: bytes  # SHA-1 digest of the piece's data.


def valid_piece_data(piece: Piece, data: bytes) -> bool:
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

    def read(self, folder: pathlib.Path) -> bytes:
        """Read the chunk's data from the file system."""
        with (folder / self.file.path).open("rb") as f:
            f.seek(self.begin - self.file.begin)
            return f.read(self.length)

    def write(self, folder: pathlib.Path, data: bytes) -> None:
        """Write the chunk's data to the file system."""
        with (folder / self.file.path).open("rb+") as f:
            f.seek(self.begin - self.file.begin)
            begin = self.begin - self.piece.begin
            f.write(data[begin : begin + self.length])


def make_chunks(
    pieces: Sequence[Piece], files: Sequence[File]
) -> dict[Piece, list[Chunk]]:
    """Map each element of `pieces` to a list of its `Chunk`s."""
    result: dict[Piece, list[Chunk]] = {piece: [] for piece in pieces}
    i = 0
    j = 0
    begin = 0
    while i < len(files) and j < len(pieces):
        file = files[i]
        piece = pieces[j]
        file_end = file.begin + file.length
        piece_end = piece.begin + piece.length
        end = min(file_end, piece_end)
        if file_end <= piece_end:
            i += 1
        if piece_end <= file_end:
            j += 1
        result[piece].append(Chunk(file, piece, begin, end - begin))
        begin = end
    return result


def yield_available_pieces(
    pieces: Sequence[Piece], folder: pathlib.Path, files: Sequence[File]
) -> Generator[Piece, None, None]:
    chunks = make_chunks(pieces, files)
    for piece in pieces:
        data: list[bytes] = []
        for chunk in chunks[piece]:
            try:
                data.append(chunk.read(folder))
            except FileNotFoundError:
                continue
        if valid_piece_data(piece, b"".join(data)):
            yield piece


@dataclasses.dataclass(frozen=True)
class Block:
    """Block metadata."""

    piece: Piece
    begin: int  # Relative offset.
    length: int


def yield_blocks(piece: Piece) -> Generator[Block, None, None]:
    block_length = 2**14
    for begin in range(0, piece.length, block_length):
        yield Block(piece, begin, min(block_length, piece.length - begin))


@dataclasses.dataclass
class Metadata:
    """The information contained in a `.torrent` file.

    Supports files with multiple trackers, as specified in [BEP 0012].

    [BEP 0012]: http://bittorrent.org/beps/bep_0012.html
    """

    info_hash: bytes
    announce_list: list[str]
    pieces: list[Piece]
    files: list[File]

    @classmethod
    def from_bytes(cls, raw_metadata: bytes) -> Self:
        """Parse a `.torrent` file."""
        d: Any = bencoding.decode(raw_metadata)

        announce_list: list[str] = []
        if b"announce-list" in d:
            # I'm ignoring the tiered structure because I'll be requesting peers
            # from every tracker; it's also not supported by magnet links.
            for tier in d[b"announce-list"]:
                for raw_announce in tier:
                    announce_list.append(raw_announce.decode())
        elif b"announce" in d:
            announce_list.append(d[b"announce"].decode())

        info = d[b"info"]

        files: list[File] = []
        if b"length" in info:
            # Single file mode.
            length = info[b"length"]
            files.append(File(0, length, pathlib.Path(info[b"name"].decode())))
        else:
            # Multiple file mode.
            length = 0
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
        pieces: list[Piece] = []
        begin = 0
        for i, j in enumerate(range(0, len(hashes), 20)):
            end = min(length, begin + piece_length)
            pieces.append(Piece(i, begin, end - begin, hashes[j : j + 20]))
            begin = end

        return cls(
            hashlib.sha1(bencoding.raw_val(raw_metadata, b"info")).digest(),
            announce_list,
            pieces,
            files,
        )
