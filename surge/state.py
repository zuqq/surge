import collections


class StateMachineMixin:
    def __init__(self):
        # Maps `state` to the set of `Future`s waiting for `state`.
        self._waiters = collections.defaultdict(set)

        # Maps `(start_state, message_type)` to `(side_effect, end_state)`.
        # Transitions from a state to itself with no side effect are implicit.
        # The `Open` state is treated separately because the handshake message
        # doesn't have a length prefix.
        self._transition = {}

    @property
    def _state(self):
        return self.__class__

    @_state.setter
    def _state(self, state):
        self.__class__ = state

    def _feed(self, message):
        start_state = self._state
        (side_effect, end_state) = self._transition.get(
            (start_state, type(message)), (None, start_state)
        )
        self._state = end_state
        if side_effect is not None:
            side_effect(message)
        if end_state in self._waiters:
            for waiter in self._waiters.pop(end_state):
                waiter.set_result(None)
