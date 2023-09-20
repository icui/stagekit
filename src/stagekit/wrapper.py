from __future__ import annotations
from typing import ParamSpec, Awaitable, Dict, Callable, Any, Literal, cast, overload
from importlib import import_module

from .stage import Stage, current_stage
from .context import Context
from .config import config


# current running context
ctx = Context()

# type alias for StageFunc.match
Match = Dict[str, None | Callable[[Any], Any]]


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
    match: Match

    # display name in command `stagekit log`
    name: Callable[[dict], str] | None

    def __init__(self, func: Callable, rerun: bool | Literal['auto'], match: None | Match, name: Callable[[dict], str] | None):
        self.func = func
        self.rerun = rerun
        self.match = {}
        self.name = name

        if match:
            self.match.update(match)
    
    def __call__(self, *args, **kwargs):
        current = current_stage()

        # run as a child stage of current stage
        stage = Stage(self, args, kwargs, ctx._chdir, current.version if current else 0)

        if current is None:
            from .main import run
            return run(stage, False)
        
        else:
            stage.parent = current
            return current.progress(stage, ctx)

    def __getstate__(self):
        return {'m': self.func.__module__, 'n': self.func.__name__}

    def __setstate__(self, state: dict):
        f: StageFunc = getattr(import_module(state['m']), state['n'])
        self.func = f.func
        self.rerun = f.rerun
        self.match = f.match
        self.name = f.name

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
def stage(*, rerun: bool | Literal['auto'] = 'auto', match: None | Match = None, name: Callable[[dict], str] | None = None) -> Callable[[Callable[Q, Any]], Callable[Q, Awaitable[Any]]]: ...

def stage(func: Callable[P, Any] | None = None, *, rerun: bool | Literal['auto'] = config['rerun_strategy'], match: Match | None = None, name: Callable[[dict], str] | None = None) -> Callable[P, Awaitable[Any]] | Callable[[Callable[Q, Any]], Callable[Q, Awaitable[Any]]]:
    """Function wrapper that creates a stage to execute the function.

    Args:
        func (Callable): Function to create stage from.
        rerun (bool | Literal['auto']): Whether or not to re-run existing stage function.
        match (Match | None): Dict containing custom function to determine if a parameter is the same as that from an older version.
            Set the value of a parameter name to None if the parameter should be ignored for matching. Defaults to None
    """
    if func is None:
        return cast(Any, lambda f: StageFunc(f, rerun, match, name))
    
    return cast(Any, StageFunc(func, config['rerun_strategy'], None, None))


@stage
async def call(cmd: str, cwd: str | None = None):
    """Call a shell command (wrapped in a stage).

    Args:
        cmd (str): Shell command to be called.
        cwd (str | None, optional): Working direction of the command. Defaults to None.
    """
    from asyncio import create_subprocess_shell

    process = await create_subprocess_shell(cmd, cwd=cwd)
    await process.communicate()
