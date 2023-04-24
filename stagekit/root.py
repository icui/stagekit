from __future__ import annotations
from typing import TYPE_CHECKING
from traceback import format_exc
from sys import stderr

from .directory import Directory

if TYPE_CHECKING:
    from .stage import Stage


root = Directory('.')


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
        await stage.execute()
    
    except Exception:
        err = format_exc()
        print(err, file=stderr)

    root.dump(stage, 'stagekit.pickle')
