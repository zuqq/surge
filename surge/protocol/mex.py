import asyncio

from . import _extension
from . import _metadata
from . import _peer
from . import base


class Protocol(base.Closed):
    def __init__(self, info_hash, peer_id):
        super().__init__(info_hash, peer_id)

        self._ut_metadata = None
        self.metadata_size = asyncio.get_event_loop().create_future()

        def handshake_cb(message):
            self._ut_metadata = message.ut_metadata
            self.metadata_size.set_result(message.metadata_size)

        def data_cb(message):
            self._data.put_nowait((message.index, message.data))

        self._transition = {
            (base.Established, _extension.Handshake): (handshake_cb, Choked),
            (Choked, _peer.Unchoke): (None, Unchoked),
            (Choked, _metadata.Data): (data_cb, Choked),
            (Unchoked, _peer.Choke): (None, Choked),
            (Unchoked, _metadata.Data): (data_cb, Unchoked),
        }


class Choked(base.Established):
    async def request(self, index):
        if self._exc is not None:
            raise self._exc
        waiter = asyncio.get_running_loop().create_future()
        self._waiters[Unchoked].add(waiter)
        self._write(_peer.Interested())
        await waiter
        self._write(
            _peer.ExtensionProtocol(
                _extension.Metadata(
                    _metadata.Request(index),
                    self._ut_metadata,
                )
            )
        )


class Unchoked(base.Established):
    async def request(self, index):
        if self._exc is not None:
            raise self._exc
        self._write(
            _peer.ExtensionProtocol(
                _extension.Metadata(
                    _metadata.Request(index),
                    self._ut_metadata,
                )
            )
        )
