import asyncio
import unittest.mock

import pytest

from surge import actor


@pytest.mark.asyncio
async def test_start_stop():
    a = actor.Actor()

    assert not a.running

    await a.start()

    assert a.running

    await a.stop()

    assert not a.running


class ActorSpec(actor.Actor):
    # Public attributes that are set in `actor.Actor.__init__`.
    parent = None
    children = None
    result = None


@pytest.mark.asyncio
async def test_start_stop_propagates():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=ActorSpec)
    parent.children.add(child)

    await parent.start()

    child.start.assert_awaited()

    await parent.stop()

    child.stop.assert_awaited()


class PlannedException(Exception):
    pass


class CrashingActor(actor.Actor):
    async def _main(self):
        raise PlannedException


@pytest.mark.asyncio
async def test_exception_raised():
    actor = CrashingActor()

    await actor.start()
    with pytest.raises(PlannedException):
        await actor
    assert actor.crashed

    await actor.stop()


@pytest.mark.asyncio
async def test_crash_reported():
    parent = unittest.mock.Mock(spec_set=ActorSpec)
    child = CrashingActor(parent)

    child.parent = parent
    await child.start()
    try:
        await child
    except PlannedException:
        pass

    parent.report_crash.assert_called_with(child)

    await child.stop()


@pytest.mark.asyncio
async def test_uncaught_crash():
    parent = actor.Actor()
    child = CrashingActor(parent)
    parent.children.add(child)

    await parent.start()
    try:
        await child
    except PlannedException:
        pass

    with pytest.raises(RuntimeError):
        await parent

    await parent.stop()


@pytest.mark.asyncio
async def test_crash_propagates():
    parent = unittest.mock.Mock(spec_set=ActorSpec)
    child = actor.Actor(parent)
    grandchild = CrashingActor(child)
    child.children.add(grandchild)

    await child.start()
    try:
        await grandchild
    except PlannedException:
        pass
    try:
        await child
    except RuntimeError:
        pass

    parent.report_crash.assert_called_with(child)

    await child.stop()


@pytest.mark.asyncio
async def test_supervisor():
    parent = actor.Actor()
    child = unittest.mock.Mock(spec_set=ActorSpec)
    parent.children.add(child)

    await parent.start()
    parent.report_crash(child)
    try:
        await parent
    except RuntimeError:
        pass

    child.stop.assert_awaited()

    await parent.stop()
