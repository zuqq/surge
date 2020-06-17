from __future__ import annotations
from typing import Any, Dict, Generator, Iterable, List

import dataclasses
import hashlib
import os

from . import bencoding


@dataclasses.dataclass(eq=True, frozen=True)
class File:
    begin: int
    length: int
    path: str

    @classmethod
    def from_dict(cls, begin: int, d: Dict[bytes, Any]) -> File:
        length = d[b"length"]
        path = os.path.join(*(part.decode() for part in d[b"path"]))
        return cls(begin, length, path)


def build_file_tree(folder: str, files: Iterable[File]):
    for file in files:
        path = os.path.join(folder, file.path)
        tail, _ = os.path.split(path)
        if tail:
            os.makedirs(tail, exist_ok=True)
        with open(path, "a+b") as f:
            f.truncate(file.length)


@dataclasses.dataclass(eq=True, frozen=True)
class Piece:
    index: int
    begin: int
    length: int
    hash: bytes  # SHA-1 digest of the piece's data.


def valid(piece: Piece, data: bytes) -> bool:
    return len(data) == piece.length and hashlib.sha1(data).digest() == piece.hash


def available_pieces(pieces: Iterable[Piece],
                     files: Iterable[File],
                     folder: str) -> Generator[Piece, None, None]:
    for piece in pieces:
        data = []
        for chunk in chunks(files, piece):
            path = os.path.join(folder, chunk.file.path)
            try:
                with open(path, "rb") as f:
                    f.seek(chunk.begin - chunk.file.begin)
                    data.append(f.read(chunk.length))
            except FileNotFoundError:
                continue
        if valid(piece, b"".join(data)):
            yield piece


@dataclasses.dataclass(eq=True, frozen=True)
class Chunk:
    """The part of a `Piece` belonging to a single `File`."""

    file: File
    piece: Piece
    begin: int
    length: int


def chunks(files: Iterable[File], piece: Piece) -> Generator[Chunk, None, None]:
    for file in files:
        begin = max(file.begin, piece.begin)
        end = min(file.begin + file.length, piece.begin + piece.length)
        if begin <= end:
            yield Chunk(file, piece, begin, end - begin)


def write(folder: str, chunk: Chunk, data: bytes):
    path = os.path.join(folder, chunk.file.path)
    with open(path, "rb+") as f:
        f.seek(chunk.begin - chunk.file.begin)
        begin = chunk.begin - chunk.piece.begin
        f.write(data[begin : begin + chunk.length])


@dataclasses.dataclass(eq=True, frozen=True, order=True)
class Block:
    piece: Piece
    begin: int
    length: int


def blocks(piece: Piece) -> Generator[Block, None, None]:
    block_size = 2 ** 14
    for begin in range(0, piece.length, block_size):
        yield Block(piece, begin, min(block_size, piece.length - begin))


@dataclasses.dataclass
class Metadata:
    announce_list: List[str]
    length: int
    piece_length: int
    pieces: List[Piece]
    folder: str
    files: List[File]

    @classmethod
    def from_bytes(cls, raw_meta: bytes) -> Metadata:
        d = bencoding.decode(raw_meta)
        announce_list = []
        if b"announce-list" in d:  # See BEP 12.
            # I'm ignoring the tiered structure because I'll be requesting peers
            # from every tracker anyway.
            for tier in d[b"announce-list"]:
                for raw_announce in tier:
                    announce_list.append(raw_announce.decode())
        elif b"announce" in d:
            # According to BEP 12, "announce" should be ignored if
            # "announce-list" is present.
            announce_list.append(d[b"announce"].decode())

        info = d[b"info"]

        if b"length" in info:
            # Single file mode.
            length = info[b"length"]
            files = [File(0, length, info[b"name"].decode())]
            folder = ""
        else:
            # Multiple file mode.
            length = 0
            files = []
            for f in info[b"files"]:
                file = File.from_dict(length, f)
                files.append(file)
                length += file.length
            folder = info[b"name"].decode()

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
