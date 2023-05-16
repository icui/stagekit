from __future__ import annotations
from typing import ParamSpec, Awaitable, Callable, Any, Literal, Iterable, TYPE_CHECKING, overload
from importlib import import_module
import asyncio

from .stage import Stage, current_stage
from .task import Task

if TYPE_CHECKING:
    from .context import Context


# current running context
_ctx: Context


def _task_factory(self, coro, context=None):
    """Add a custom property to asyncio.Task to store the stage a task is created from."""
    task = Task(coro, loop=self, context=context) # type: ignore

    try:
        task._sk_stage = asyncio.current_task()._sk_stage # type: ignore

    except:
        pass

    return task


class StageFunc:
    """Custom serializer for function decorated by @stage to avoid pickle error."""
    # function decorated by @stage
    func: Callable

    # whether or not to re-run existing stage when called
    # True: always re-run
    # False: never re-run
    # 'auto': re-run only if stage has at least one child stage (because by design, stages with child stages should be cheap to run)
    rerun: bool | Literal['auto']

    # ignore argument(s) in comparing and saving
    skip: None | str | Iterable[str]

    def __init__(self, func: Callable, rerun: bool | Literal['auto'], skip: None | str | Iterable[str]):
        self.func = func
        self.rerun = rerun
        self.skip = skip
    
    def __call__(self, *args, **kwargs):
        current = current_stage()

        if current is None:
            # if root stage is None, run as root stage
            from .main import main, ctx

            global _ctx
            _ctx = ctx

            stage = Stage(self, args, kwargs, None, 0)
            with asyncio.Runner() as runner:
                loop = runner.get_loop()
                loop.set_task_factory(_task_factory)
                runner.run(main(stage))

        else:
            # if root stage exists, run as a child of current stage
            stage = Stage(self, args, kwargs, _ctx._chdir, current.version)
            stage.parent = current
            return current.progress(stage, _ctx)

    def __getstate__(self):
        return {'m': self.func.__module__, 'n': self.func.__name__, 'r': self.rerun, 's': self.skip}

    def __setstate__(self, state: dict):
        self.func = getattr(import_module(state['m']), state['n']).func
        self.rerun = state['r']
        self.skip = state['s']

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
def stage(*, rerun: bool | Literal['auto'] = 'auto', skip: None | str | Iterable[str] = None) -> Callable[[Callable[Q, Any]], Callable[Q, Awaitable[Any]]]: ...

def stage(func: Callable[P, Any] | None = None, *, rerun: bool | Literal['auto'] = 'auto', skip: None | str | Iterable[str] = None) -> \
    Callable[[Callable[Q, Any]], Callable[Q, Awaitable[Any]]] | Callable[P, Awaitable[Any]]:
    """Function wrapper that creates a stage to execute the function.
        If decorated function is a class method, be sure to define __eq__ of the class, otherwise the progress may not be saved.

    Args:
        func (Callable): Function to create stage from.
        rerun (bool | Literal['auto']): Whether or not to re-run existing stage function.
    """
    if func is None:
        return lambda f: StageFunc(f, rerun, skip) # type: ignore
    
    return StageFunc(func, 'auto', None) # type: ignore
