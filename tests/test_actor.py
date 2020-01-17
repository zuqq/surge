import asyncio
import unittest.mock

import pytest

from surge import actor


@pytest.mark.asyncio
async def test_start():
    a = actor.Actor()

    await a.start()
    assert a._running


@pytest.mark.asyncio
async def test_start_stop():
    a = actor.Actor()

    await a.start()
    assert a._running

    await a.stop()
    assert not a._running
    assert a._crashed


@pytest.mark.asyncio
async def test_stop_children():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=actor.Actor)
    await parent.start()
    await parent.spawn_child(child)

    await parent.stop()
    child.stop.assert_awaited()


@pytest.mark.asyncio
async def test_crash_unsupervised():
    # TODO
    pass


@pytest.mark.asyncio
async def test_crash_reporting():
    parent = unittest.mock.Mock(spec_set=actor.Actor)
    child = actor.Actor()
    child._on_start = unittest.mock.AsyncMock(side_effect=RuntimeError)
    child._monitor_children = unittest.mock.AsyncMock()
    await child.start(parent)

    # Finish all scheduled tasks.
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    await asyncio.gather(*tasks, return_exceptions=True)

    assert child._crashed
    parent.report_crash.assert_called_with(child)


@pytest.mark.asyncio
async def test_spawn_child():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=actor.Actor)
    type(child)._parent = unittest.mock.PropertyMock()

    await parent.spawn_child(child)
    assert child in parent._children
    child.start.assert_awaited()
    child._parent._assert_called_once_with(parent)


@pytest.mark.asyncio
async def test_crashed_children_put():
    parent = actor.Actor()
    parent._monitor_children = unittest.mock.AsyncMock()
    child = unittest.mock.Mock(spec_set=actor.Actor)

    await parent.start()
    await parent.spawn_child(child)

    with unittest.mock.patch.object(parent._crashed_children, "put_nowait") as mock_put:
        parent.report_crash(child)
    mock_put.assert_called_once_with(child)


@pytest.mark.asyncio
async def test_crashed_children_get():
    parent = actor.Actor(is_supervisor=True)
    parent._on_child_crash = unittest.mock.AsyncMock()
    child = unittest.mock.Mock(spec_set=actor.Actor)

    await parent.start()
    await parent.spawn_child(child)

    with unittest.mock.patch.object(parent._crashed_children, "get") as mock_get:
        mock_get.side_effect = (child,)

        # Finish all scheduled tasks.
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        await asyncio.gather(*tasks, return_exceptions=True)

    child.stop.assert_awaited()
    parent._on_child_crash.assert_awaited()


@pytest.mark.asyncio
async def test_crash_is_not_supervisor():
    # TODO
    pass
