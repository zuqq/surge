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
    # See BEP 12.
    announce_list: List[str]
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

    missing_pieces: Optional[List[Piece]] = None  # For resuming the download.

    @classmethod
    def from_bytes(cls, metainfo_file):
        decoded = bencoding.decode(metainfo_file)
        params = {}
        trackers = []
        if b"announce" in decoded:
            trackers.append(decoded[b"announce"].decode("utf-8"))
        if b"announce-list" in decoded:
            for tier in decoded[b"announce-list"]:
                for raw_tracker in tier:
                    trackers.append(raw_tracker.decode("utf-8"))
        params["announce_list"] = trackers
        raw_info = bencoding.raw_val(metainfo_file, b"info")
        params["info_hash"] = hashlib.sha1(raw_info).digest()
        params.update(_parse_info(decoded[b"info"]))
        return cls(**params)


@dataclasses.dataclass(eq=True, frozen=True)
class Peer:
    address: str
    port: int
    id: Optional[bytes] = None

    @classmethod
    def from_bytes(cls, raw_peer):
        return cls(
            ".".join(str(b) for b in raw_peer[:4]),
            int.from_bytes(raw_peer[4:], "big"),
        )

    @classmethod
    def from_dict(cls, peer_dict):
        return cls(
            peer_dict[b"ip"].decode("utf-8"), peer_dict[b"port"], peer_dict[b"peer id"],
        )
