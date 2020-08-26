import collections

from surge import messages
from surge import mex

from ._example import Example


class TestMex(Example):
    def test_mex(self):
        other_peer_id = b"\xbe\xbb\xe9R\t\xcb!\xffu\xd1\x10\xc3X\\\x05\xab\x945\xee\x9a"
        transducer = mex.mex(self.info_hash, self.peer_id)

        event = transducer.send(None)
        self.assertIsInstance(event, mex.Write)
        self.assertIsInstance(event.message, messages.Handshake)

        event = transducer.send(None)
        self.assertIsInstance(event, mex.NeedHandshake)

        event = transducer.send(messages.Handshake(self.info_hash, other_peer_id))
        self.assertIsInstance(event, mex.Write)
        self.assertIsInstance(event.message, messages.ExtensionHandshake)
        ut_metadata = event.message.ut_metadata

        event = transducer.send(None)
        self.assertIsInstance(event, mex.NeedMessage)

        outbox = collections.deque()
        message = messages.ExtensionHandshake(3, len(self.raw_info))
        with self.assertRaises(StopIteration) as cm:
            while True:
                event = transducer.send(message)
                message = None
                if isinstance(event, mex.Write):
                    if isinstance(event.message, messages.MetadataRequest):
                        i = event.message.index
                        outbox.append(
                            messages.MetadataData(
                                i,
                                len(self.raw_info),
                                self.raw_info[i * 2 ** 14 : (i + 1) * 2 ** 14],
                                ut_metadata,
                            )
                        )
                elif isinstance(event, mex.NeedMessage):
                    if outbox:
                        message = outbox.popleft()
        self.assertEqual(cm.exception.value, self.raw_info)
