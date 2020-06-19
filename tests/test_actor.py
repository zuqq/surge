import asyncio
import unittest.mock

import pytest

from surge import actor


@pytest.mark.asyncio
async def test_start_stop():
    a = actor.Actor()
    assert not a.running
    await a.start()
    await asyncio.sleep(1)
    assert a.running
    await a.stop()
    assert not a.running


class ActorSpec(actor.Actor):
    # Public attributes that are set in `actor.Actor.__init__`.
    parent = None
    children = set()
    running = False


@pytest.mark.asyncio
async def test_start_stop_propagates():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=ActorSpec)
    parent.children.add(child)
    await parent.start()
    await asyncio.sleep(1)
    child.start.assert_awaited()
    await parent.stop()
    child.stop.assert_awaited()


@pytest.mark.asyncio
async def test_spawn_child():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=ActorSpec)
    await parent.start()
    await asyncio.sleep(1)
    await parent.spawn_child(child)
    assert child in parent.children
    child.start.assert_awaited()
    await parent.stop()


@pytest.mark.asyncio
async def test_crash_reported():
    class CrashingActor(actor.Actor):
        def __init__(self, parent):
            super().__init__(parent)

            self._coros.add(self._main())

        async def _main(self):
            raise Exception

    parent = unittest.mock.Mock(spec_set=ActorSpec)
    child = CrashingActor(parent)
    await child.start()
    await asyncio.sleep(1)
    parent.report_crash.assert_called_with(child)
    await child.stop()


@pytest.mark.asyncio
async def test_crashed_actor_stopped():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=ActorSpec)
    await parent.start()
    parent.report_crash(child)
    await asyncio.sleep(1)
    child.stop.assert_awaited()
    await parent.stop()
