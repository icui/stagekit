from __future__ import annotations
from typing import ParamSpec, Awaitable, Callable, Any, Literal, TYPE_CHECKING, overload
from importlib import import_module
import asyncio
from functools import wraps

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
    # 'auto': re-run only if stage has at least one child stage (because by design, stages with child stages should be cheap to run)
    rerun: bool | Literal['auto']

    # class instance if function is a class method
    obj: object | None

    def __init__(self, func: Callable, rerun: bool | Literal['auto'], obj: object | None):
        self.func = func
        self.rerun = rerun
        self.obj = obj
    
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
        if self.obj:
            return {'o': self.obj, 'f': self.func.__name__, 'r': self.rerun}

        return {'m': self.func.__module__, 'n': self.func.__name__, 'r': self.rerun}

    def __setstate__(self, state: dict):
        if 'o' in state:
            self.func = getattr(state['o'], state['f']).__wrapped__
            self.obj = state['o']
        
        else:
            self.func = getattr(import_module(state['m']), state['n']).func
            self.obj = None
        
        self.rerun = state['r']

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
def stage(*, rerun: bool | Literal['auto']) -> Callable[[Callable[Q, Any]], Callable[Q, Awaitable[Any]]]: ...

def stage(func: Callable[P, Any] | None = None, *, rerun: bool | Literal['auto'] = 'auto') -> \
    Callable[[Callable[Q, Any]], Callable[Q, Awaitable[Any]]] | Callable[P, Awaitable[Any]]:
    """Function wrapper that creates a stage to execute the function.
        If decorated function is a class method, be sure to define __eq__ of the class, otherwise the progress may not be saved.

    Args:
        func (Callable): Function to create stage from.
        rerun (bool | Literal['auto']): Whether or not to re-run existing stage function.
    """
    if func is None:
        return lambda f: _wrap(f, rerun)
    
    return _wrap(func, 'auto')

def _wrap(func: Callable[P, Any], rerun: bool | Literal['auto']) -> Any:
    if '.' in func.__qualname__:
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            return StageFunc(func, 'auto', self)(*args, **kwargs)
        
        return wrapper

    return StageFunc(func, rerun, None)
