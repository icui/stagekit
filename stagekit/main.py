from __future__ import annotations
from traceback import format_exc
from sys import stderr
import asyncio

from .directory import Directory
from .stage import Stage
from .context import Context
from .execute import STAGE_IN_SUBPROCESS

root = Directory('.')
ctx = Context()


async def save_current():
    """Save current state to stagekit.pickle one second later."""
    if not ctx._saving and ctx._current:
        stage = ctx._current

        while stage.parent:
            stage = stage.parent

        ctx._saving = True
        await asyncio.sleep(1)
        await save(stage)


async def save(stage: Stage):
    """Save a state to stagekit.pickle instantly."""
    if not STAGE_IN_SUBPROCESS:
        root.dump(stage, '_stagekit.pickle')
        await asyncio.sleep(1)

        try:
            s = root.load('_stagekit.pickle')
            assert s.config == stage.config

        except:
            pass

        else:
            root.mv('_stagekit.pickle', 'stagekit.pickle')


async def main(stage: Stage):
    """Execute main stage.

    Args:
        stage (Stage): Main stage.
    """
    if root.has('stagekit.pickle'):
        # restore from saved state
        s = root.load('stagekit.pickle')
        if s.config == stage.config:
            stage = s

    try:
        output = await stage.execute(ctx)
        if output is not None:
            print(output)

    except Exception as e:
        err = format_exc()
        print(err, file=stderr)

        if ctx._current:
            ctx._current.error = e

    await save(stage)
