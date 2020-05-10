import asyncio
import unittest.mock

import pytest

from surge import actor


@pytest.mark.asyncio
async def test_start():
    a = actor.Actor()

    await a.start()

    assert a.running


@pytest.mark.asyncio
async def test_start_stop():
    a = actor.Actor()

    await a.start()
    await a.stop()

    assert not a.running


@pytest.mark.asyncio
async def test_spawn_child():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=actor.Actor)

    await parent.start()
    await parent.spawn_child(child)

    assert child in parent.children
    child.start.assert_awaited()


@pytest.mark.asyncio
async def test_stop_propagates():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=actor.Actor)

    await parent.start()
    await parent.spawn_child(child)
    await parent.stop()

    child.stop.assert_awaited()


@pytest.mark.asyncio
async def test_crash_is_reported():
    parent = unittest.mock.Mock(spec_set=actor.Actor)
    child = actor.Actor()
    child._main_coro = unittest.mock.AsyncMock(side_effect=RuntimeError)

    await child.start(parent)
    # Finish all scheduled tasks.
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    await asyncio.gather(*tasks, return_exceptions=True)

    assert child.crashed
    parent.report_crash.assert_called_with(child)


@pytest.mark.asyncio
async def test_crash_propagates():
    parent = unittest.mock.Mock(spec_set=actor.Actor)
    child = actor.Actor()
    grandchild = actor.Actor()
    grandchild._main_coro = unittest.mock.AsyncMock(side_effect=RuntimeError)

    await child.start(parent)
    await child.spawn_child(grandchild)
    # Finish all scheduled tasks.
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    await asyncio.gather(*tasks, return_exceptions=True)

    parent.report_crash.assert_called_with(child)
