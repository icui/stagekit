from __future__ import annotations
from traceback import format_exc
from sys import stderr
import asyncio
from importlib import import_module
from typing import overload, Literal

from .stage import Stage, current_stage
from .runner import InsufficientWalltime
from .config import config
from .wrapper import ctx
from .task import task_factory
from .cache import load_cache


async def _execute(stage: Stage | None, main: bool):
    ctx._chdir = None

    output = None

    for s in load_cache():
        if stage is None:
            if len(s.args) == 0 and len(s.kwargs) == 0:
                stage = s
                stage.func = stage.func.load() # type: ignore
                stage.flat = False
                break

        elif s.renew(stage):
            stage = s
            break

    try:
        if stage is None:
            return

        # execute root stage
        task = asyncio.current_task()
        task._sk_stage = stage # type: ignore

        if stage.done:
            output = stage.result
        
        else:
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

    if stage is not None and not stage.flat:
        from .data.data import save_data

        ctx._save(stage)
        save_data()
    
    return output


@overload
async def run(stage: Stage | None, main: Literal[True]): ...

@overload
async def run(stage: Stage, main: Literal[False]): ...

def run(stage: Stage | None, main: bool):
    """Create a new asyncio event loop to execute a function wrapped in @stage.

    Args:
        stage (Stage): Function to be executed (must be wrapped in @stage).
        main (bool): Whether the event loop is the main program (called by `stagekit run`).
    """
    with asyncio.Runner() as runner:
        loop = runner.get_loop()
        loop.set_task_factory(task_factory)
        return runner.run(_execute(stage, main))
