import collections

from surge import messages
from surge import protocol

from ._example import Example


class TestProtocol(Example):
    def test_piece_download(self):
        other_peer_id = b"\xbe\xbb\xe9R\t\xcb!\xffu\xd1\x10\xc3X\\\x05\xab\x945\xee\x9a"
        state = protocol.State(50)
        transducer = protocol.base(self.pieces, self.info_hash, self.peer_id, state)

        event = transducer.send(None)
        self.assertIsInstance(event, protocol.Write)
        self.assertIsInstance(event.message, messages.Handshake)

        event = transducer.send(None)
        self.assertIsInstance(event, protocol.NeedHandshake)

        event = transducer.send(messages.Handshake(self.info_hash, other_peer_id))
        self.assertIsInstance(event, protocol.NeedMessage)

        outbox = collections.deque()
        piece = self.pieces[0]
        data = self.data[0]
        to_download = [piece]
        message = messages.Bitfield.from_indices({0}, len(self.pieces))
        while True:
            event = transducer.send(message)
            message = None
            if isinstance(event, protocol.Write):
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
            elif isinstance(event, protocol.Result):
                break
            elif isinstance(event, protocol.NeedPiece):
                if to_download:
                    state.add_piece(to_download.pop())
                else:
                    state.requesting = False
            elif isinstance(event, protocol.NeedMessage):
                if outbox:
                    message = outbox.popleft()
        self.assertEqual(event.piece, piece)
        self.assertEqual(event.data, data)
