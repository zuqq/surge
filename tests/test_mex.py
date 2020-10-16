import collections
import hashlib
import unittest

from surge import bencoding
from surge import messages
from surge.mex import _transducer


class TestMex(unittest.TestCase):
    def test_mex(self):
        raw_info = bencoding.encode(
            {
                b"length": 1,
                b"name": b"a",
                b"piece length": 2 ** 18,
                b"pieces": b"\xa0\xf1I\n \xd0!\x1c\x99{D\xbc5~\x19r\xde\xab\x8a\xe3",
            }
        )
        info_hash = hashlib.sha1(raw_info).digest()
        our_peer_id = b"\x88\x07 \x7f\x00d\xedr J\x13w~.\xb2_P\xf3\xf82"
        their_peer_id = b"\xbe\xbb\xe9R\t\xcb!\xffu\xd1\x10\xc3X\\\x05\xab\x945\xee\x9a"

        transducer = _transducer.mex(info_hash, our_peer_id)

        event = transducer.send(None)
        self.assertIsInstance(event, _transducer.Send)
        self.assertIsInstance(event.message, messages.Handshake)

        event = transducer.send(None)
        self.assertIsInstance(event, _transducer.ReceiveHandshake)

        event = transducer.send(messages.Handshake(1 << 20, info_hash, their_peer_id))
        self.assertIsInstance(event, _transducer.Send)
        self.assertIsInstance(event.message, messages.ExtensionHandshake)
        ut_metadata = event.message.ut_metadata

        event = transducer.send(None)
        self.assertIsInstance(event, _transducer.ReceiveMessage)

        outbox = collections.deque()
        message = messages.ExtensionHandshake(3, len(raw_info))
        with self.assertRaises(StopIteration) as cm:
            while True:
                event = transducer.send(message)
                message = None
                if isinstance(event, _transducer.Send):
                    if isinstance(event.message, messages.MetadataRequest):
                        i = event.message.index
                        outbox.append(
                            messages.MetadataData(
                                i,
                                len(raw_info),
                                raw_info[i * 2 ** 14 : (i + 1) * 2 ** 14],
                                ut_metadata,
                            )
                        )
                elif isinstance(event, _transducer.ReceiveMessage):
                    if outbox:
                        message = outbox.popleft()
        self.assertEqual(cm.exception.value, raw_info)
