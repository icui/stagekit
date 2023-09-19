from __future__ import annotations
from traceback import format_exc
from sys import stderr
from os.path import join
import asyncio
from importlib import import_module
from typing import overload, Literal, TYPE_CHECKING

from .stage import Stage, current_stage
from .runner import InsufficientWalltime
from .config import config, PATH_WORKSPACE
from .wrapper import ctx
from .task import task_factory

if TYPE_CHECKING:
    from .wrapper import StageFunc


async def _execute(stage: Stage | None, main: bool):
    for src in config['modules']:
        import_module(src)
    
    path_pkl = join(PATH_WORKSPACE, 'stagekit.pickle')
    output = None

    if ctx.root.has(path_pkl):
        # restore from saved state
        s = ctx.root.load(path_pkl)
        if stage is None or s == stage:
            stage = s

    try:
        if stage is None:
            return

        # execute root stage
        task = asyncio.current_task()
        task._sk_stage = stage # type: ignore

        output = await stage.execute(ctx)

        if main and output is not None:
            print(output)

    except Exception as e:
        print(format_exc(), file=stderr)

        if isinstance(e, InsufficientWalltime):
            # TODO
            pass

        elif current := current_stage():
            current.error = e

    if stage is not None:
        ctx._save(stage)
    
    return output


@overload
async def run(stage: Stage | None, main: Literal[True]): ...

@overload
async def run(stage: Stage, main: Literal[False]): ...

def run(stage: Stage | None, main: bool):
    """Create a new asyncio event loop to execute a function wrapped in @stage.

    Args:
        stage (Stage): Function to be executed (must be wrapped in @stage).
        main (bool): Whether main event loop is the main program.
            Determines the format of stagekit.pickle.
            If True, which means that run is called by `stagekit run`, only this stage is saved to stagekit.pickle.
            If False, which means that fun is called by @stage wrapper dynamically, all stages called in this way are saved to stagekit_run.pickle.
    """
    with asyncio.Runner() as runner:
        loop = runner.get_loop()
        loop.set_task_factory(task_factory)
        return runner.run(_execute(stage, main))
