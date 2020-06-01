import enum
import secrets
import struct


class Message(enum.Enum):
    CONNECT = 0
    ANNOUNCE = 1


def connect(trans_id):
    return struct.pack(">ql4s", 0x41727101980, Message.CONNECT.value, trans_id)


def announce(trans_id, conn_id, params):
    return struct.pack(
        ">8sl4s20s20sqqqlL4slH",
        conn_id,
        Message.ANNOUNCE.value,
        trans_id,
        params.info_hash,
        params.peer_id,
        params.downloaded,
        params.left,
        params.uploaded,
        0,
        0,
        secrets.token_bytes(4),
        -1,
        6881,
    )


def parse_connect(data):
    return struct.unpack(">l4s8s", data)


def parse_announce(data):
    return struct.unpack(">l4slll", data[:20])
