from typing import List, Optional

import dataclasses
import hashlib
import os

from . import bencoding


@dataclasses.dataclass(eq=True, frozen=True)
class File:
    index: int
    length: int
    path: str


def ensure_files_exist(folder, files):
    """Create any files that do not exist and truncate the ones that already exist."""
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
class FileChunk:
    """The part of a piece that belongs to a single file."""

    file: File
    piece: Piece
    file_offset: int
    piece_offset: int
    length: int


def piece_to_chunks(pieces, files):
    """Return a dictionary mapping a piece to its chunks."""
    result = {piece: [] for piece in pieces}
    file_index = 0
    file = files[file_index]
    file_offset = 0
    for piece in pieces:
        piece_offset = 0
        while piece_offset < piece.length:
            length = min(piece.length - piece_offset, file.length - file_offset)
            result[piece].append(
                FileChunk(file, piece, file_offset, piece_offset, length)
            )
            piece_offset += length
            file_offset += length
            if file_offset == file.length and file_index < len(files) - 1:
                file_index += 1
                file = files[file_index]
                file_offset = 0
    return result


def missing_pieces(pieces, files, folder):
    """Return a list of those pieces that are not present in the download folder."""
    result = set(pieces)
    piece_to_chunks_ = piece_to_chunks(pieces, files)
    for piece in pieces:
        chunk_data = []
        for chunk in piece_to_chunks_[piece]:
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
    """Return a list of the blocks of the given piece."""
    result = []
    for piece_offset in range(0, piece.length, block_length):
        length = min(block_length, piece.length - piece_offset)
        result.append(Block(piece, piece_offset, length))
    return result


def _parse_file_list(file_list):
    files = []
    for index, file_dict in enumerate(file_list):
        length = file_dict[b"length"]
        path_parts = [part.decode("utf-8") for part in file_dict[b"path"]]
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


def _parse_info(info):
    result = {}
    name = info[b"name"].decode("utf-8")
    if b"length" in info:
        # Single file mode.
        result["files"] = [File(0, info[b"length"], name)]
        result["folder"] = ""
    else:
        # Multiple file mode.
        result["files"] = _parse_file_list(info[b"files"])
        result["folder"] = name
    result["length"] = sum(file.length for file in result["files"])
    result["piece_length"] = info[b"piece length"]
    result["pieces"] = _parse_hashes(
        info[b"pieces"], result["length"], result["piece_length"]
    )
    return result


@dataclasses.dataclass
class Metainfo:
    announce: str  # TODO: This should be a list.
    length: int
    piece_length: int
    pieces: List[Piece]
    folder: str
    files: List[File]

    # Tracker parameters. The first two are also needed for peer messaging.
    info_hash: bytes
    peer_id: Optional[bytes] = None
    port: int = 6881
    uploaded: int = 0
    downloaded: int = 0
    left: int = 0
    event: str = "started"
    compact: int = 1  # See BEP 23.

    # For resuming the download.
    missing_pieces: Optional[List[Piece]] = None

    @classmethod
    def from_bytes(cls, metainfo_file):
        decoded = bencoding.decode(metainfo_file)
        params = {}
        params["announce"] = decoded[b"announce"].decode("utf-8")
        raw_info = bencoding.raw_val(metainfo_file, b"info")
        params["info_hash"] = hashlib.sha1(raw_info).digest()
        params.update(_parse_info(decoded[b"info"]))
        return cls(**params)


@dataclasses.dataclass(eq=True, frozen=True)
class Peer:
    address: str
    port: int
    id: Optional[bytes]

    @classmethod
    def from_bytes(cls, raw_peer):
        return cls(
            ".".join(str(b) for b in raw_peer[:4]),
            int.from_bytes(raw_peer[4:], "big"),
            None,
        )

    @classmethod
    def from_dict(cls, peer_dict):
        return cls(
            peer_dict[b"ip"].decode("utf-8"), peer_dict[b"port"], peer_dict[b"peer id"],
        )
