import collections
import functools
import struct
import unittest

from surge import messages
from surge import metadata
from surge import protocol

from ._example import Example


class TestProtocol(Example):
    # Identifier for the peer.
    other_peer_id = b"\xbe\xbb\xe9R\t\xcb!\xffu\xd1\x10\xc3X\\\x05\xab\x945\xee\x9a"

    def test_piece_download(self):
        state = protocol.DownloadState(self.pieces, 50)
        transducer = protocol.base(
            self.pieces, self.info_hash, self.peer_id, state.available
        )
        next_event = functools.partial(protocol.next_event, state, transducer)
        # Queue for outgoing messages. We process them whenever `state.send`
        # returns `protocol.NeedMessage`.
        queue = collections.deque()
        piece = self.pieces[0]
        data = self.data[0]
        # Flag to make sure that we only download one piece.
        sent_piece = False

        # Unroll the first couple of iterations of the loop because the
        # handshake and bitfield are only sent once.
        event = next_event(None)
        self.assertIsInstance(event, protocol.Send)
        sent = event.message
        self.assertIsInstance(sent, messages.Handshake)

        event = next_event(None)
        self.assertIsInstance(event, protocol.NeedMessage)

        event = next_event(messages.Handshake(self.info_hash, self.other_peer_id))
        self.assertIsInstance(event, protocol.NeedMessage)

        event = next_event(messages.Bitfield.from_indices({0}, len(self.pieces)))
        self.assertIsInstance(event, protocol.Send)
        sent = event.message
        self.assertIsInstance(sent, messages.Interested)
        message = messages.Unchoke()

        while True:
            event = next_event(message)
            message = None
            if isinstance(event, protocol.Send):
                queue.append(event.message)
            elif isinstance(event, protocol.Result):
                break
            elif isinstance(event, protocol.NeedPiece):
                if not sent_piece:
                    state.add_piece(piece)
                    sent_piece = True
                else:
                    state.requesting = False
            elif isinstance(event, protocol.NeedMessage):
                while queue:
                    sent = queue.popleft()
                    if isinstance(sent, messages.Request):
                        block = sent.block(self.pieces)
                        self.assertEqual(block.piece, piece)
                        message = messages.Block.from_block(
                            block, data[block.begin : block.begin + block.length]
                        )
                        break

        self.assertEqual(event.piece, piece)
        self.assertEqual(event.data, data)
