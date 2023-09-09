from __future__ import annotations
from traceback import format_exc
from sys import stderr
import asyncio
from importlib import import_module
from typing import TYPE_CHECKING

from .stage import Stage, current_stage
from .runner import InsufficientWalltime
from .config import config, PATH_PICKLE
from .wrapper import ctx
from .task import Task

if TYPE_CHECKING:
    from .wrapper import StageFunc


def _task_factory(self, coro, context=None):
    """Add a custom property to asyncio.Task to store the stage a task is created from."""
    task = Task(coro, loop=self, context=context) # type: ignore

    try:
        task._sk_stage = asyncio.current_task()._sk_stage # type: ignore

    except:
        pass

    return task


async def _execute(stage: Stage):
    """Execute main stage.

    Args:
        stage (Stage): Main stage.
    """
    for src in config['modules']:
        import_module(src)

    if ctx.root.has(PATH_PICKLE):
        # restore from saved state
        s = ctx.root.load(PATH_PICKLE)
        if s == stage:
            stage = s

    try:
        # execute root stage
        task = asyncio.current_task()
        task._sk_stage = stage # type: ignore

        output = await stage.execute(ctx)
        if output is not None:
            print(output)

    except Exception as e:
        print(format_exc(), file=stderr)

        if isinstance(e, InsufficientWalltime):
            # TODO
            pass

        elif current := current_stage():
            current.error = e

    ctx._save(stage)


def main(func: StageFunc):
    stage = Stage(func, [], {}, None, 0)

    with asyncio.Runner() as runner:
        loop = runner.get_loop()
        loop.set_task_factory(_task_factory)
        runner.run(_execute(stage))
