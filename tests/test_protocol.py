import collections

from surge import _transducer
from surge import messages

from ._example import Example


class TestProtocol(Example):
    def test_piece_download(self):
        other_peer_id = b"\xbe\xbb\xe9R\t\xcb!\xffu\xd1\x10\xc3X\\\x05\xab\x945\xee\x9a"
        state = _transducer.State(50)
        transducer = _transducer.base(self.pieces, self.info_hash, self.peer_id, state)

        event = transducer.send(None)
        self.assertIsInstance(event, _transducer.Send)
        self.assertIsInstance(event.message, messages.Handshake)

        event = transducer.send(None)
        self.assertIsInstance(event, _transducer.ReceiveHandshake)

        event = transducer.send(messages.Handshake(self.info_hash, other_peer_id))
        self.assertIsInstance(event, _transducer.ReceiveMessage)

        outbox = collections.deque()
        piece = self.pieces[0]
        data = self.data[0]
        to_download = [piece]
        message = messages.Bitfield.from_indices({0}, len(self.pieces))
        while True:
            event = transducer.send(message)
            message = None
            if isinstance(event, _transducer.Send):
                if isinstance(event.message, messages.Interested):
                    outbox.append(messages.Unchoke())
                elif isinstance(event.message, messages.Request):
                    block = event.message.block(self.pieces)
                    self.assertEqual(block.piece, piece)
                    outbox.append(
                        messages.Block.from_block(
                            block, data[block.begin : block.begin + block.length]
                        )
                    )
            elif isinstance(event, _transducer.PutPiece):
                break
            elif isinstance(event, _transducer.GetPiece):
                if to_download:
                    state.add_piece(to_download.pop())
                else:
                    state.requesting = False
            elif isinstance(event, _transducer.ReceiveMessage):
                if outbox:
                    message = outbox.popleft()
        self.assertEqual(event.piece, piece)
        self.assertEqual(event.data, data)
