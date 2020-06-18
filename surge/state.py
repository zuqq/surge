import collections


class StateMachine:
    def __init__(self):
        self.state = None
        # Maps `(start_state, event_type)` to `(side_effect, end_state)`.
        # Transitions from a state to itself with no side effect are implicit.
        self._transition = {}
        # Maps `event_type` to a set of futures.
        self._waiters = collections.defaultdict(set)

    def feed(self, event):
        event_type = type(event)
        (side_effect, self.state) = self._transition.get(
            (self.state, event_type), (None, self.state)
        )
        if side_effect is not None:
            side_effect(event)
        if event_type not in self._waiters:
            return
        for waiter in self._waiters.pop(event_type):
            if not waiter.done():
                waiter.set_result(event)

    def add_waiter(self, waiter, event_type):
        self._waiters[event_type].add(waiter)
