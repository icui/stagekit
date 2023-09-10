from __future__ import annotations
from traceback import format_exc
from sys import stderr
from os.path import join
import asyncio
from importlib import import_module
from typing import TYPE_CHECKING

from .stage import Stage, current_stage
from .runner import InsufficientWalltime
from .config import config, PATH_WORKSPACE
from .wrapper import ctx
from .task import task_factory

if TYPE_CHECKING:
    from .wrapper import StageFunc


async def _execute(stage: Stage):
    """Execute main stage.

    Args:
        stage (Stage): Main stage.
    """
    for src in config['modules']:
        import_module(src)
    
    path_pkl = join(PATH_WORKSPACE, 'stagekit.pickle')

    if ctx.root.has(path_pkl):
        # restore from saved state
        s = ctx.root.load(path_pkl)
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
        loop.set_task_factory(task_factory)
        runner.run(_execute(stage))
