"""Implementation of the UDP tracker protocol.

Specification: [BEP 0015]

The UDP tracker protocol is a lightweight alternative to the original HTTP-based
way of communicating with trackers. It is a simple two-step protocol built on
top of UDP, with a custom retransmission mechanism using exponential backoff.

[BEP 0015]: http://bittorrent.org/beps/bep_0015.html
"""

from typing import ClassVar

import dataclasses
import secrets
import struct

from . import _metadata


@dataclasses.dataclass
class ConnectRequest:
    value: ClassVar[int] = 0
    transaction_id: bytes

    def to_bytes(self):
        return struct.pack(">ql4s", 0x41727101980, self.value, self.transaction_id)


@dataclasses.dataclass
class AnnounceRequest:
    value: ClassVar[int] = 1
    transaction_id: bytes
    connection_id: bytes
    parameters: _metadata.Parameters

    def to_bytes(self) -> bytes:
        return struct.pack(
            ">8sl4s20s20sqqqlL4slH",
            self.connection_id,
            self.value,
            self.transaction_id,
            self.parameters.info_hash,
            self.parameters.peer_id,
            self.parameters.downloaded,
            self.parameters.left,
            self.parameters.uploaded,
            0,
            0,
            secrets.token_bytes(4),
            -1,
            self.parameters.port,
        )


@dataclasses.dataclass
class ConnectResponse:
    value: ClassVar[int] = 0
    connection_id: bytes

    @classmethod
    def from_bytes(cls, data):
        _, _, connection_id = struct.unpack(">l4s8s", data)
        return cls(connection_id)


@dataclasses.dataclass
class AnnounceResponse:
    value: ClassVar[int] = 1
    result: _metadata.Result

    @classmethod
    def from_bytes(cls, data):
        _, _, interval, _, _ = struct.unpack(">l4slll", data[:20])
        return cls(_metadata.Result.from_bytes(interval, data[20:]))


def parse(data):
    if len(data) < 4:
        raise ValueError("Not enough bytes.")
    value = int.from_bytes(data[:4], "big")
    if value == ConnectResponse.value:
        return ConnectResponse.from_bytes(data)
    if value == AnnounceResponse.value:
        return AnnounceResponse.from_bytes(data)
    raise ValueError("Unkown message identifier.")
