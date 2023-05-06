from __future__ import annotations
from traceback import format_exc
from sys import stderr
import asyncio

from .stage import Stage
from .task import setup_task
from .context import Context, current_stage
from .execute import STAGE_IN_SUBPROCESS

# context of root directory as directory utility
root = Context()

# context used by stages
ctx = Context()


async def checkpoint():
    """Save root stage to stagekit.pickle one second later."""
    if ctx._saving:
        return

    if stage := current_stage():
        ctx._saving = True

        while stage.parent:
            stage = stage.parent

        await asyncio.sleep(1)

        if ctx._saving:
            await save(stage)


async def save(stage: Stage):
    """Save a stage to stagekit.pickle."""
    if not STAGE_IN_SUBPROCESS:
        root.dump(stage, '_stagekit.pickle')
        await asyncio.sleep(1)

        try:
            # verify saved state
            s = root.load('_stagekit.pickle')
            assert s == stage

        except:
            pass

        else:
            root.mv('_stagekit.pickle', 'stagekit.pickle')

        ctx._saving = False


async def main(stage: Stage):
    """Execute main stage.

    Args:
        stage (Stage): Main stage.
    """
    setup_task()

    if root.has('stagekit.pickle'):
        # restore from saved state
        s = root.load('stagekit.pickle')
        if s == stage:
            stage = s

    try:
        # execute root stage
        task = asyncio.current_task()
        task._sk_stage = stage # type: ignore

        output = await stage.execute(ctx, checkpoint)
        if output is not None:
            print(output)

    except Exception as e:
        err = format_exc()
        print(err, file=stderr)

        if current := current_stage():
            current.error = e

    await save(stage)
