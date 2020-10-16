import collections
import hashlib
import unittest

from surge import _metadata
from surge import _transducer
from surge import bencoding
from surge import messages


class TestProtocol(unittest.TestCase):
    def test_piece_download(self):
        data = b"s"
        piece = _metadata.Piece(0, 0, 1, hashlib.sha1(data).digest())
        raw_info = bencoding.encode(
            {
                b"length": 1,
                b"name": b"a",
                b"piece length": 2 ** 18,
                b"pieces": piece.hash,
            }
        )
        info_hash = hashlib.sha1(raw_info).digest()
        our_peer_id = b"\xa7\x88\x06\x8b\xeb6i~=//\x1e\xc8\x1d\xbb\x12\x023\xa58"
        their_peer_id = b"\xbe\xbb\xe9R\t\xcb!\xffu\xd1\x10\xc3X\\\x05\xab\x945\xee\x9a"

        state = _transducer.State(50)
        transducer = _transducer.base([piece], info_hash, our_peer_id, state)

        event = transducer.send(None)
        self.assertIsInstance(event, _transducer.Send)
        self.assertIsInstance(event.message, messages.Handshake)

        event = transducer.send(None)
        self.assertIsInstance(event, _transducer.ReceiveHandshake)

        event = transducer.send(messages.Handshake(0, info_hash, their_peer_id))
        self.assertIsInstance(event, _transducer.ReceiveMessage)

        outbox = collections.deque()
        got_piece = False
        message = messages.Bitfield.from_indices({0}, 1)
        while True:
            event = transducer.send(message)
            message = None
            if isinstance(event, _transducer.Send):
                if isinstance(event.message, messages.Interested):
                    outbox.append(messages.Unchoke())
                elif isinstance(event.message, messages.Request):
                    message = event.message
                    if message.index == piece.index:
                        block = _metadata.Block(piece, message.begin, message.length)
                        outbox.append(
                            messages.Block.from_block(
                                block, data[block.begin : block.begin + block.length]
                            )
                        )
            elif isinstance(event, _transducer.PutPiece):
                break
            elif isinstance(event, _transducer.GetPiece):
                if got_piece:
                    state.requesting = False
                else:
                    state.add_piece(piece)
                    got_piece = True
            elif isinstance(event, _transducer.ReceiveMessage):
                if outbox:
                    message = outbox.popleft()
        self.assertEqual(event.piece, piece)
        self.assertEqual(event.data, data)
