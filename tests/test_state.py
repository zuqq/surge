import asyncio
import unittest.mock

import pytest

from surge import state


def test_state():
    class Closed:
        pass

    class Open:
        pass

    class OpenCommand:
        pass

    class CloseCommand:
        pass

    on_open = unittest.mock.Mock()
    on_close = unittest.mock.Mock()

    waiter = asyncio.Future()
    waiter.set_result = unittest.mock.Mock()

    class StateMachine(state.StateMachine):
        def __init__(self):
            super().__init__()

            self.add_waiter(waiter, CloseCommand)

            self._transition = {
                (Closed, OpenCommand): (on_open, Open),
                (Open, CloseCommand): (on_close, Closed),
            }

            self.state = Closed

    state_machine = StateMachine()

    state_machine.feed(OpenCommand())
    assert state_machine.state is Open
    on_open.assert_called()

    state_machine.feed(OpenCommand())
    assert state_machine.state is Open

    cmd = CloseCommand()
    state_machine.feed(cmd)
    assert state_machine.state is Closed
    on_close.assert_called()
    waiter.set_result.assert_called_with(cmd)
