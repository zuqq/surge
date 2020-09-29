import secrets

from surge.tracker import _metadata
from surge.tracker import _udp

from ._example import Example


class TestUDP(Example):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.params = _metadata.Parameters(cls.info_hash, secrets.token_bytes(20))
        cls.connection_id = b"\x1c\xe3\xc2\x0bP\x88\x96\xd1"

    def test_successful_transaction(self):
        # Successful transaction, with a one second delay between the answers.
        transducer = _udp.udp(self.params)

        message, _ = transducer.send(None)
        self.assertIsInstance(message, _udp.ConnectRequest)

        message, _ = transducer.send((_udp.ConnectResponse(self.connection_id), 0))
        self.assertIsInstance(message, _udp.AnnounceRequest)

        interval = 1800
        peers = [_metadata.Peer("127.0.0.1", 6969)]
        with self.assertRaises(StopIteration) as cm:
            transducer.send(
                (_udp.AnnounceResponse(_metadata.Result(interval, peers)), 1)
            )
        response = cm.exception.value
        self.assertEqual(response.interval, interval)
        self.assertEqual(response.peers, peers)

    def test_timeout(self):
        transducer = _udp.udp(self.params)

        message, timeout = transducer.send(None)
        self.assertIsInstance(message, _udp.ConnectRequest)

        time = timeout + 1
        for _ in range(1, 9):
            message, timeout = transducer.send((None, time))
            self.assertIsInstance(message, _udp.ConnectRequest)
            time += timeout + 1

        with self.assertRaises(_udp.ProtocolError):
            transducer.send((None, time))

    def test_connection_id_timeout(self):
        transducer = _udp.udp(self.params)

        message, _ = transducer.send(None)
        self.assertIsInstance(message, _udp.ConnectRequest)

        message, _ = transducer.send((_udp.ConnectResponse(self.connection_id), 0))
        self.assertIsInstance(message, _udp.AnnounceRequest)

        message, _ = transducer.send((None, 60))
        self.assertIsInstance(message, _udp.ConnectRequest)
