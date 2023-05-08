from __future__ import annotations
from typing import ParamSpec, Awaitable, Callable, Any, Literal, TYPE_CHECKING, overload
from importlib import import_module
import asyncio

from .stage import Stage, current_stage

if TYPE_CHECKING:
    from .context import Context


# current running context
_ctx: Context


class StageFunc:
    """Custom serializer for function decorated by @stage to avoid pickle error."""
    # function decorated by @stage
    func: Callable

    # whether or not to re-run existing stage when called
    # True: always re-run
    # False: never re-run
    # 'auto': re-run only if stage has at least one child stage (because by design,
    #   stages with child stages should be cheap to run)
    rerun: bool | Literal['auto']

    def __init__(self, func: Callable, rerun: bool | Literal['auto']):
        self.func = func
        self.rerun = rerun
    
    def __call__(self, *args, **kwargs):
        current = current_stage()

        if current is None:
            # if root stage is None, run as root stage
            from .main import main, ctx

            global _ctx
            _ctx = ctx

            stage = Stage(self, args, kwargs, None)
            asyncio.run(main(stage))

        else:
            # if root stage exists, run as a child of current stage
            stage = Stage(self, args, kwargs, _ctx._chdir)
            stage.parent = current
            return current.progress(stage, _ctx)

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
Q = ParamSpec('Q')


@overload
def stage(func: Callable[P, Any]) -> Callable[P, Awaitable[Any]]: ...

@overload
def stage(func: None, *, rerun: bool | Literal['auto']) -> Callable[[Callable[Q, Any]], Callable[Q, Awaitable[Any]]]: ...

def stage(func: Callable[P, Any] | None = None, *, rerun: bool | Literal['auto'] = 'auto') -> \
    Callable[[Callable[Q, Any]], Callable[Q, Awaitable[Any]]] | Callable[P, Awaitable[Any]]:
    """Function wrapper that creates a stage to execute the function.

    Args:
        func (Callable): Function to create stage from.
        rerun (bool | Literal['auto']): Whether or not to re-run existing stage function.
    """
    if func is None:
        def wrapper(f):
            return StageFunc(f, rerun)

        return wrapper # type: ignore

    return StageFunc(func, 'auto') #type: ignore


def setup_wrapper(ctx: Context, checkpoint: Callable):
    """Set global """
