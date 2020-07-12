import unittest

from ._example import Example
from surge.tracker import _metadata
from surge.tracker import _udp


class UDPTest(Example):
    connection_id = b"\x1c\xe3\xc2\x0bP\x88\x96\xd1"

    def test_successful(self):
        transducer = _udp.udp(self.params)
        message, _ = transducer.send(None)
        self.assertIsInstance(message, _udp.ConnectRequest)
        message, _ = transducer.send(_udp.ConnectResponse(self.connection_id))
        self.assertIsInstance(message, _udp.AnnounceRequest)
        interval = 1800
        peers = [_metadata.Peer("127.0.0.1", 6969)]
        with self.assertRaises(StopIteration) as cm:
            message, _ = transducer.send(
                _udp.AnnounceResponse(_metadata.Response(interval, peers))
            )
        response = cm.exception.value
        self.assertEqual(response.interval, interval)
        self.assertEqual(response.peers, peers)
