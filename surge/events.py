import dataclasses

from . import messages


# Events -----------------------------------------------------------------------


class Event:
    pass


@dataclasses.dataclass
class Receive(Event):
    message: messages.Message


@dataclasses.dataclass
class Send(Event):
    message: messages.Message


# Sending requests needs to be treated separately because the driver chooses
# which block to request.
class Request(Event):
    pass


class Listen(Event):
    pass


# States -----------------------------------------------------------------------


class State:
    def __init__(self, name):
        self._name = name

    def __repr__(self):
        return self._name


_CLOSED = State("CLOSED")
_OPEN = State("OPEN")
_WAITING = State("WAITING")
_CHOKED = State("CHOKED")
_INTERESTED = State("INTERESTED")
_UNCHOKED = State("UNCHOKED")


# Transducer -------------------------------------------------------------------


class Transducer:
    def __init__(self, info_hash, peer_id):
        self._message = None

        # Maps `(state, message_type)` to `(relay, new_state)`, where `relay` is
        # a boolean indicating whether or not to relay the message to the
        # driver. If a pair `(state, message_type)` does not appear here, then
        # it is a no-op.
        self._receive = {
            (_UNCHOKED, messages.Choke): (True, _CHOKED),
            (_CHOKED, messages.Unchoke): (False, _UNCHOKED),
            (_INTERESTED, messages.Unchoke): (False, _UNCHOKED),
            (_CHOKED, messages.Have): (True, _CHOKED),
            (_UNCHOKED, messages.Have): (True, _UNCHOKED),
            (_WAITING, messages.Bitfield): (True, _CHOKED),
            (_CHOKED, messages.Block): (True, _CHOKED),
            (_UNCHOKED, messages.Block): (True, _UNCHOKED),
            (_OPEN, messages.Handshake): (True, _WAITING),
        }

        # Maps `state` to `(message, new_state)`.
        self._send = {
            _CLOSED: (messages.Handshake(info_hash, peer_id), _OPEN),
            _CHOKED: (messages.Interested(), _INTERESTED),
        }

        self._state = _CLOSED

    def __iter__(self):
        return self

    def __next__(self) -> Event:
        # Note that this iterator never raises `StopIteration`.

        # Check if there's something to receive.
        if (message := self._message) is not None:
            self._message = None
            relay, self._state = self._receive.get(
                (self._state, type(message)), (False, self._state)
            )
            if relay:
                return Receive(message)

        # Check if there's something to send.
        message, self._state = self._send.get(self._state, (None, self._state))
        if message is not None:
            return Send(message)

        # See `Request`.
        if self._state is _UNCHOKED:
            return Request()

        # If there's nothing to receive nor send, then we read from the peer.
        return Listen()

    def feed(self, message: messages.Message):
        if self._message is not None:
            raise ValueError("Already have an unprocess message.")
        self._message = message
