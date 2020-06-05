import asyncio
import unittest.mock

import pytest

from surge import actor


@pytest.mark.asyncio
async def test_start():
    a = actor.Actor()

    assert not a.running

    await a.start()

    assert a.running

    await a.stop()


@pytest.mark.asyncio
async def test_start_stop():
    a = actor.Actor()

    assert not a.running

    await a.start()
    await a.stop()

    assert not a.running


class ActorSpec(actor.Actor):
    parent = None
    children = set()


@pytest.mark.asyncio
async def test_spawn_child():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=ActorSpec)

    await parent.start()
    await parent.spawn_child(child)

    assert child in parent.children
    child.start.assert_awaited()

    await parent.stop()


@pytest.mark.asyncio
async def test_spawn_while_stopped():
    parent = actor.Actor()
    child = actor.Actor()
    with pytest.raises(actor.InvalidState):
        await parent.spawn_child(child)


@pytest.mark.asyncio
async def test_stop_propagates():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=ActorSpec)
    child.parent = None

    await parent.start()
    await parent.spawn_child(child)
    await parent.stop()

    child.stop.assert_awaited()


class PlannedException(Exception):
    pass


class CrashingActor(actor.Actor):
    async def _main(self):
        raise PlannedException


@pytest.mark.asyncio
async def test_exception_raised():
    child = CrashingActor()
    await child.start()

    with pytest.raises(PlannedException):
        await child.result
    assert child.crashed

    await child.stop()


@pytest.mark.asyncio
async def test_crash_is_reported():
    parent = unittest.mock.Mock(spec_set=ActorSpec)
    child = CrashingActor()
    child.parent = parent
    await child.start()
    try:
        await child.result
    except PlannedException:
        pass

    parent.report_crash.assert_called_with(child)

    await child.stop()


@pytest.mark.asyncio
async def test_uncaught_crash():
    parent = actor.Actor()
    child = CrashingActor()
    await parent.start()
    await parent.spawn_child(child)
    try:
        await child.result
    except PlannedException:
        pass

    with pytest.raises(actor.UncaughtCrash):
        await parent.result

    await parent.stop()


@pytest.mark.asyncio
async def test_crash_propagates():
    parent = unittest.mock.Mock(spec_set=ActorSpec)
    child = actor.Actor()
    grandchild = CrashingActor()

    child.parent = parent
    await child.start()
    await child.spawn_child(grandchild)
    try:
        await grandchild.result
    except PlannedException:
        pass
    try:
        await child.result
    except actor.UncaughtCrash:
        pass

    parent.report_crash.assert_called_with(child)

    await child.stop()
