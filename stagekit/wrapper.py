from typing import ParamSpec, Awaitable, Callable, Any
from importlib import import_module
import asyncio

from .stage import Stage, create_task
from .context import current_stage
from .main import main, ctx, checkpoint


class StageFunc:
    """Custom serializer for function decorated by @stage to avoid pickle error."""
    # function decorated by @stage
    func: Callable

    def __init__(self, func: Callable):
        self.func = func
    
    def __call__(self, *args, **kwargs):
        current = current_stage()
        stage = Stage(self, args, kwargs, ctx._chdir)
        stage.parent = current

        if current is None:
            # if root stage is None, run as root stage
            asyncio.run(main(stage))

        else:
            # if root stage exists, run as a child of current stage
            stage.index = current.step + len(current.pending)
            task = create_task(current.progress(stage, ctx, checkpoint))
            task._sk_stage = stage
            current.pending.append(stage)
            return task

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


def gather(*coros: Awaitable):
    """Run multiple stages or tasks concurrently."""
    current = current_stage()

    if current is None:
        raise RuntimeError('gather can only be run inside a stage')

    if current.step < len(current.history):
        s = current.history[current.step]
