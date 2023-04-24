from __future__ import annotations
from typing import Any, List, Mapping, Iterable, Callable, Tuple, ParamSpec, cast
import asyncio
from importlib import import_module

from .context import ctx
from .root import main


# typing for Stage configuration
StageConfig = Tuple[Callable, Iterable[Any], Mapping[str, Any]]


class Stage:
    """Wrapper of a function to save execution progress."""
    # [0]: wrapped function
    # [1]: arguments passed to self.func
    # [2]: keyword argumentd passed to self.func
    config: StageConfig

    # index of current child stage
    step: int = 0

    # executed child stages
    history: List[Stage]

    # parent stage
    parent: Stage | None = None

    # return value of self.func
    result: Any = None

    # main function successfully executed
    done = False

    def __init__(self, config: StageConfig):
        self.config = config
        self.history = []
        self.data = {}

    async def execute(self):
        """Execute main function."""
        # change context to self
        self.parent = ctx._current
        ctx._current = self

        # initialize state
        self.step = 0
        self.done = False

        result = self.config[0].func(*self.config[1], **self.config[2])
        if asyncio.iscoroutine(result):
            result = await result
        
        self.result = result
        self.done = True

        # restore context to parent stage
        ctx._current = self.parent

    async def progress(self, config: StageConfig):
        """Compare and execute a child step.

        Args:
            args (StageConfig): Arguments of the child step.
        """
        if (self.step < len(self.history)):
            stage = self.history[self.step]

            # skip if stage is already created or executed
            if stage.config == config:
                if not stage.done:
                    await stage.execute()

                self.step += 1
                return stage.result

        self.history = self.history[:self.step]
        stage = Stage(config)
        self.history.append(stage)
        await stage.execute()
        self.step += 1

        return stage.result


class StageFunc:
    """Class for wrapped stage function to avoid pickle error with decorated function."""
    func: Callable

    def __init__(self, func: Callable):
        self.func = func
    
    def __call__(self, *args, **kwargs):
        config = (self, args, kwargs)

        if ctx._current is None:
            # execute as root stage
            asyncio.run(main(Stage(config)))

        else:
            # execute as a child of current stage
            return ctx._current.progress(config)
    
    def __getstate__(self):
        return {'m': self.func.__module__, 'n': self.func.__name__}

    def __setstate__(self, state: Mapping):
        self.func = getattr(import_module(state['m']), state['n']).func
    
    def __eq__(self, func):
        if isinstance(func, StageFunc):
            return self.__getstate__() == func.__getstate__()

        return False


P = ParamSpec('P')

def stage(func: Callable[P, Any]) -> Callable[P, Any]:
    """Function wrapper that creates a stage to execute the function.

    Args:
        func (Callable): Function to create stage from.
    """
    return cast(Any, StageFunc(func))
