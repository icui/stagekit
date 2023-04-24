from __future__ import annotations
from typing import Any, List, Mapping, Iterable, Callable, Tuple, ParamSpec
import asyncio
from importlib import import_module
from collections.abc import Awaitable

from .root import main


# stage being executed
_currentStage: Stage | None = None


class Context:
    """Getter of keyword arguments that also inherits from parent stages."""
    def __getattr__(self, key: str):
        current = _currentStage
        
        while current:
            if key in current.data:
                return current.data[key]

            if key in current.config[2]:
                return current.config[2][key]
            
            current = current.parent
        
        return None

    def __setattr__(self, key: str, val: Any):
        if _currentStage:
            _currentStage.data[key] = val

ctx = Context()


class StageFunc:
    """Custom serializer for stage function to avoid pickle error with decorators."""
    func: Callable

    def __init__(self, func: Callable):
        self.func = func
    
    def __call__(self, *args, **kwargs):
        config = (self, args, kwargs)

        if _currentStage is None:
            asyncio.run(main(Stage(config), ctx))

        else:
            return _currentStage.progress(config)
    
    def __getstate__(self):
        return {'m': self.func.__module__, 'n': self.func.__name__}

    def __setstate__(self, state: dict):
        self.func = getattr(import_module(state['m']), state['n']).func
    
    def __eq__(self, func):
        if isinstance(func, StageFunc):
            return self.__getstate__() == func.__getstate__()

        return False


# typing for Stage configuration
# [0]: wrapped function
# [1]: arguments passed to self.func
# [2]: keyword argumentd passed to self.func
StageConfig = Tuple[StageFunc, Iterable[Any], Mapping[str, Any]]


class Stage:
    """Wrapper of a function to save execution progress."""
    # stage function and arguments
    config: StageConfig

    # index of current child stage
    step: int = 0

    # executed child stages
    history: List[Stage]

    # MPI or subprocess function calls awaiting execution
    scheduled: List[Callable]

    # parent stage
    parent: Stage | None = None

    # return value of self.func
    result: Any = None

    # main function successfully executed
    done = False

    # error occured during execution
    error: Exception | None = None

    # data that can be accessed through ctx
    data: dict

    def __init__(self, config: StageConfig):
        self.config = config
        self.history = []
        self.data = {}

    async def execute(self):
        """Execute main function."""
        # change context to self
        global _currentStage
        self.parent = _currentStage
        _currentStage = self

        # initialize state
        self.step = 0
        self.done = False

        result = self.config[0].func(*self.config[1], **self.config[2])
        if asyncio.iscoroutine(result):
            result = await result
        
        self.result = result
        self.done = True

        # restore context to parent stage
        _currentStage = self.parent

        return result

    async def progress(self, config: StageConfig):
        """Compare and execute a child step.

        Args:
            args (StageConfig): Arguments of the child step.
        """
        if self.step < len(self.history):
            # skip if stage is already created or executed
            stage = self.history[self.step]

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


P = ParamSpec('P')

def stage(func: Callable[P, Any]) -> Callable[P, Awaitable[Any]]:
    """Function wrapper that creates a stage to execute the function.

    Args:
        func (Callable): Function to create stage from.
    """
    return StageFunc(func) #type: ignore


__all__ = ['stage', 'ctx']
