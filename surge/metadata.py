from __future__ import annotations
from typing import Any, Dict, List

import dataclasses
import hashlib
import os

from . import bencoding


@dataclasses.dataclass(eq=True, frozen=True)
class File:
    index: int
    length: int
    path: str

    @classmethod
    def from_dict(cls, index, d):
        length = d[b"length"]
        path = os.path.join(*(part.decode() for part in d[b"path"]))
        return cls(index, length, path)


@dataclasses.dataclass(eq=True, frozen=True)
class Piece:
    index: int
    length: int
    hash: bytes  # SHA-1 digest of the piece's data.


def valid(piece: Piece, data: bytes) -> Bool:
    return len(data) == piece.length and hashlib.sha1(data).digest() == piece.hash


@dataclasses.dataclass(eq=True, frozen=True)
class Chunk:
    """The part of `piece` belonging to `file`."""

    file: File
    piece: Piece
    file_offset: int
    piece_offset: int
    length: int


def chunks(pieces: List[Piece], files: List[File]) -> Dict[Piece, List[Chunk]]:
    """Return a dictionary mapping each piece to a list of its chunks."""
    result: Dict[Piece, List[Chunk]] = {piece: [] for piece in pieces}
    file_index = 0
    file = files[file_index]
    file_offset = 0
    for piece in pieces:
        piece_offset = 0
        while piece_offset < piece.length:
            length = min(piece.length - piece_offset, file.length - file_offset)
            result[piece].append(Chunk(file, piece, file_offset, piece_offset, length))
            piece_offset += length
            file_offset += length
            if file_offset == file.length and file_index < len(files) - 1:
                file_index += 1
                file = files[file_index]
                file_offset = 0
    return result


@dataclasses.dataclass(eq=True, frozen=True, order=True)
class Block:
    piece: Piece
    piece_offset: int
    length: int


def blocks(piece: Piece, block_length: int = 2 ** 14) -> List[Block]:
    """Return a sorted list of `piece`'s blocks.

    This is not a generator function because the caller typically also needs
    immediate access to all blocks.
    """
    result = []
    for piece_offset in range(0, piece.length, block_length):
        length = min(block_length, piece.length - piece_offset)
        result.append(Block(piece, piece_offset, length))
    return result


@dataclasses.dataclass
class Metadata:
    announce_list: List[str]  # See BEP 12.
    length: int
    piece_length: int
    pieces: List[Piece]
    folder: str
    files: List[File]

    @classmethod
    def from_bytes(cls, raw_meta: bytes) -> Metadata:
        return cls.from_dict(bencoding.decode(raw_meta))

    @classmethod
    def from_dict(cls, decoded: Dict[bytes, Any]) -> Metadata:
        announce_list = []
        if b"announce" in decoded:
            announce_list.append(decoded[b"announce"].decode())
        if b"announce-list" in decoded:
            for tier in decoded[b"announce-list"]:
                for raw_tracker in tier:
                    announce_list.append(raw_tracker.decode())
        info = decoded[b"info"]
        if b"length" in info:
            # Single file mode.
            files = [File(0, info[b"length"], info[b"name"].decode())]
            folder = ""
        else:
            # Multiple file mode.
            files = [File.from_dict(i, d) for (i, d) in enumerate(info[b"files"])]
            folder = info[b"name"].decode()
        length = sum(file.length for file in files)
        piece_length = info[b"piece length"]
        hashes = info[b"pieces"]
        pieces = []
        for offset in range(0, len(hashes), 20):
            index = offset // 20
            pieces.append(
                Piece(
                    index,
                    min(piece_length, length - index * piece_length),
                    hashes[offset : offset + 20],
                )
            )
        return cls(announce_list, length, piece_length, pieces, folder, files)
