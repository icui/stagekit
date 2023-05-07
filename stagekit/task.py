from __future__ import annotations
from typing import Coroutine, TYPE_CHECKING, cast
import asyncio

if TYPE_CHECKING:
    from .stage import Stage


class Task(asyncio.Task):
    """Task with custom properties to save stage info."""
    # the stage that created the task
    _sk_stage: Stage | None = None

    # task is created by decorator @stage
    _sk_is_stage = False


def create_task(coro: Coroutine, stage: Stage) -> Task:
    """Create a task for executing a stage."""
    task = cast(Task, asyncio.create_task(coro))
    task._sk_stage = stage
    task._sk_is_stage = True
    return task


def _task_factory(self, coro):
    """Add a custom property to asyncio.Task to store the stage a task is created from."""
    task = Task(coro, loop=self)

    try:
        task._sk_stage = asyncio.current_task()._sk_stage # type: ignore

    except:
        pass

    return task


def setup_task():
    """Set asyncio task factory."""
    loop = asyncio.get_running_loop()
    loop.set_task_factory(_task_factory)
