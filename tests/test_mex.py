import unittest

from surge import messages
from surge import mex

from ._example import Example


class TestMex(Example):
    other_peer_id = b"\xbe\xbb\xe9R\t\xcb!\xffu\xd1\x10\xc3X\\\x05\xab\x945\xee\x9a"

    def test_mex(self):
        metadata_size = len(self.raw_info)
        piece_length = 2 ** 14
        transducer = mex.mex(self.info_hash, self.peer_id)

        sent = transducer.send(None)
        self.assertIsInstance(sent, messages.Handshake)

        sent = transducer.send(messages.Handshake(self.info_hash, self.other_peer_id))
        self.assertIsInstance(sent, messages.ExtensionHandshake)

        sent = transducer.send(messages.ExtensionHandshake(3, metadata_size))
        with self.assertRaises(StopIteration) as cm:
            for i in range((metadata_size + piece_length - 1) // piece_length):
                self.assertIsInstance(sent, messages.MetadataRequest)
                self.assertEqual(sent.index, i)
                sent = transducer.send(
                    messages.MetadataData(
                        i,
                        metadata_size,
                        self.raw_info[i * piece_length : (i + 1) * piece_length],
                    )
                )
        self.assertEqual(cm.exception.value, self.raw_info)
