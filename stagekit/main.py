from traceback import format_exc
from sys import stderr
from typing import ParamSpec, Awaitable, Callable, Any
from importlib import import_module
import asyncio

from .directory import Directory
from .context import Context
from .stage import Stage, StageConfig

root = Directory('.')
ctx = Context()


class StageFunc:
    """Custom serializer for function decorated by @stage to avoid pickle error."""
    # function decorated by @stage
    func: Callable

    def __init__(self, func: Callable):
        self.func = func
    
    def __call__(self, *args, **kwargs):
        config = (self, args, kwargs)

        # if root stage exists, run as a child of current stage
        # otherwise run as root stage
        if Stage.current is None:
            asyncio.run(main(config))

        else:
            return Stage.current.progress(config)
    
    def __getstate__(self):
        return {'m': self.func.__module__, 'n': self.func.__name__}

    def __setstate__(self, state: dict):
        self.func = getattr(import_module(state['m']), state['n']).func
    
    def __eq__(self, func):
        if isinstance(func, StageFunc):
            return self.__getstate__() == func.__getstate__()

        return False


# type of the decorated function's arguments
P = ParamSpec('P')

def stage(func: Callable[P, Any]) -> Callable[P, Awaitable[Any]]:
    """Function wrapper that creates a stage to execute the function.

    Args:
        func (Callable): Function to create stage from.
    """
    return StageFunc(func) #type: ignore


async def main(config: StageConfig):
    """Execute main stage.

    Args:
        stage (Stage): Main stage.
    """
    stage = Stage(config)

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

        if Stage.current:
            Stage.error = e

    root.dump(stage, 'stagekit.pickle')
