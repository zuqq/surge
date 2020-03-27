from typing import List, Optional

import dataclasses
import hashlib
import os
import secrets

from . import bencoding


@dataclasses.dataclass(eq=True, frozen=True)
class File:
    index: int
    length: int
    path: str


def ensure_files_exist(folder, files):
    for file in files:
        full_path = os.path.join(folder, file.path)
        tail, _ = os.path.split(full_path)
        if tail:
            os.makedirs(tail, exist_ok=True)
        with open(full_path, "a+b") as f:
            f.truncate(file.length)


@dataclasses.dataclass(eq=True, frozen=True)
class Piece:
    index: int
    length: int
    hash: bytes


@dataclasses.dataclass(eq=True, frozen=True)
class Chunk:
    """The part of `piece` belonging to `file`."""

    file: File
    piece: Piece
    file_offset: int
    piece_offset: int
    length: int


def piece_to_chunks(pieces, files):
    """Return a dictionary mapping each piece to a list of its chunks."""
    result = {piece: [] for piece in pieces}
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


def missing_pieces(pieces, files, folder):
    """Return a list of those pieces that are not present in `folder`."""
    result = set(pieces)
    chunks = piece_to_chunks(pieces, files)
    for piece in pieces:
        chunk_data = []
        for chunk in chunks[piece]:
            file_path = os.path.join(folder, chunk.file.path)
            with open(file_path, "rb") as f:
                f.seek(chunk.file_offset)
                chunk_data.append(f.read(chunk.length))
        if hashlib.sha1(b"".join(chunk_data)).digest() == piece.hash:
            result.remove(piece)
    return list(result)


@dataclasses.dataclass(eq=True, frozen=True, order=True)
class Block:
    piece: Piece
    piece_offset: int
    length: int


def blocks(piece, block_length=2 ** 14):
    """Return a list of `piece`'s blocks."""
    result = []
    for piece_offset in range(0, piece.length, block_length):
        length = min(block_length, piece.length - piece_offset)
        result.append(Block(piece, piece_offset, length))
    return result


def _parse_file_list(file_list):
    files = []
    for index, file_dict in enumerate(file_list):
        length = file_dict[b"length"]
        path_parts = [part.decode() for part in file_dict[b"path"]]
        path = os.path.join(*path_parts)
        files.append(File(index, length, path))
    return files


def _parse_hashes(hashes, length, piece_length):
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
    return pieces


@dataclasses.dataclass
class Metainfo:
    announce_list: List[str]  # See BEP 12.
    length: int
    piece_length: int
    pieces: List[Piece]
    folder: str
    files: List[File]

    @classmethod
    def from_bytes(cls, raw_metainfo):
        decoded = bencoding.decode(raw_metainfo)
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
            files = _parse_file_list(info[b"files"])
            folder = info[b"name"].decode()
        length = sum(file.length for file in files)
        piece_length = info[b"piece length"]
        pieces = _parse_hashes(info[b"pieces"], length, piece_length)
        return cls(announce_list, length, piece_length, pieces, folder, files)


@dataclasses.dataclass
class TorrentState:
    info_hash: bytes
    peer_id: bytes = secrets.token_bytes(20)
    port: int = 6881
    uploaded: int = 0
    downloaded: int = 0
    left: int = 0
    event: str = "started"
    compact: int = 1  # See BEP 23.

    @classmethod
    def from_bytes(cls, raw_metainfo):
        raw_info = bencoding.raw_val(raw_metainfo, b"info")
        info_hash = hashlib.sha1(raw_info).digest()
        return cls(info_hash)


@dataclasses.dataclass(eq=True, frozen=True)
class Peer:
    address: str
    port: int
    id: Optional[bytes] = None

    @classmethod
    def from_bytes(cls, bs):
        return cls(".".join(str(b) for b in bs[:4]), int.from_bytes(bs[4:], "big"))

    @classmethod
    def from_dict(cls, d):
        return cls(d[b"ip"].decode(), d[b"port"], d[b"peer id"])
