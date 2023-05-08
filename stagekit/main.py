from __future__ import annotations
from traceback import format_exc
from sys import stderr
import asyncio

from .stage import Stage, current_stage
from .task import setup_task
from .context import Context


# context used by stages
ctx = Context()


async def main(stage: Stage):
    """Execute main stage.

    Args:
        stage (Stage): Main stage.
    """
    setup_task()

    if ctx.root.has('stagekit.pickle'):
        # restore from saved state
        s = ctx.root.load('stagekit.pickle')
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
        err = format_exc()
        print(err, file=stderr)

        if current := current_stage():
            current.error = e

    await ctx._save(stage)
