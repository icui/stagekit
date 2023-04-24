from __future__ import annotations
from typing import TYPE_CHECKING
from traceback import format_exc
from sys import stderr

from .directory import Directory

if TYPE_CHECKING:
    from .stage import Stage, Context


root = Directory('.')


async def main(stage: Stage, ctx: Context):
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
        output = await stage.execute()
        if output is not None:
            print(output)

    except Exception as e:
        err = format_exc()
        print(err, file=stderr)

        if ctx._current:
            ctx._current.error = e

    root.dump(stage, 'stagekit.pickle')


__all__ = ['root', 'main']
