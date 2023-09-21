from __future__ import annotations
from typing import Any, List, Dict, Mapping, Collection, TYPE_CHECKING
import asyncio

from .task import create_child_task
from .data.data import _data_cls

if TYPE_CHECKING:
    from .wrapper import StageFunc
    from .context import Context


class Stage:
    """Wrapper of a function to save execution progress.
        Note: Stage is intended to be a purely internal class,
        do not create a stage directly with Stage(), use decorator @stage instead."""
    # data defined by stage function accessed through ctx
    data: dict

    # stage function
    func: StageFunc

    # arguments of self.func
    args: List[Any]

    # keyword arguments of self.func
    kwargs: Dict[str, Any]

    # working directory relative to parent stage
    cwd: str | None

    # executed child stages
    history: List[Stage]

    # parent stage
    parent: Stage | None = None

    # return value of self.func
    result: Any = None

    # main function successfully executed
    done = False

    # error occured during execution
    error: Exception | None = None

    # number of times executed
    version: int = 0

    # number of times parent stage is executed
    parent_version: int

    # args and kwargs are restored from a saved state
    # a flat stage cannot be re-run unless the arguments are updated by self.renew()
    flat = False

    def __init__(self, func: StageFunc, args: Collection, kwargs: Mapping[str, Any], cwd: str | None, parent_version: int):
        self.func = func
        self.args = list(args)
        self.kwargs = dict(kwargs)
        self.cwd = cwd
        self.parent_version = parent_version

        self.history = []
        self.data = {}
    
    def __getstate__(self):
        state = self.__dict__.copy()

        if self.flat:
            return state
        
        args = state['args'] = []
        kwargs = state['kwargs'] = {}

        co_varnames = self.func.func.__code__.co_varnames

        for i, a in enumerate(self.args):
            args.append(self.flatten(co_varnames[i], a))
        
        for k, a in self.kwargs.items():
            kwargs[k] = self.flatten(k, a)
    
        state['flat'] = True

        return state

    def __eq__(self, other):
        if not isinstance(other, Stage) or self.func != other.func or self.cwd != other.cwd:
            return False
        
        if self.flat and other.flat:
            return self.args == other.args and self.kwargs == other.kwargs

        if len(self.args) != len(other.args):
            return False
        
        if len(self.kwargs) != len(other.kwargs):
            return False

        co_varnames = self.func.func.__code__.co_varnames

        for i, (arg1, arg2) in enumerate(zip(self.args, other.args)):
            k = co_varnames[i]

            if self.flatten(k, arg1) != other.flatten(k, arg2):
                return False
        
        for k in self.kwargs:
            if k not in other.kwargs:
                return False
            
            if self.flatten(k, self.kwargs[k]) != other.flatten(k, other.kwargs[k]):
                return False

        return True
    
    def __repr__(self):
        msg = ''

        if self.func.name:
            d = self.kwargs.copy()

            co_varnames = self.func.func.__code__.co_varnames

            for i in range(len(self.args)):
                d[co_varnames[i]] = self.args[i]
            
            msg += self.func.name(d)

        elif hasattr(self.func.func, '__name__'):
            msg += self.func.func.__name__
        
        else:
            msg += '<anonymous stage>'

        msg += '\n'

        children = []

        for s in self.history:
            if s.parent_version == self.version:
                children.append(s)
        
        nidx = 1 + len(str(len(children)))

        for i, s in enumerate(children):
            msg += f'{i+1})' + ' ' * (nidx - len(str(i+1))) + repr(s).replace('\n', '\n  ')
        
        return msg
    
    def flatten(self, k: str, val: Any):
        """Flatten an argument of stage function.
            Arguments:
                name (str): Argument name.
                val (Any): Argument value.
        """
        if self.flat:
            return val

        match = self.func.match

        if k in match:
            return None if match[k] is None else match[k](val) # type: ignore

        for test, data in _data_cls.items():
            if test(val):
                return data(val)
        
        return val

    def renew(self, other: Stage):
        """Compare self (previously saved stage) with a new stage and update args if needs to re-run."""
        if self == other:
            if not self.done or other.func.rerun == True or (other.func.rerun == 'auto' and len(self.history) > 0):
                # re-run existing stage if:
                # (1) stage not completed
                # (2) stage is set to alwarys re-run
                # (3) stage is set to auto re-run and stage has child stage
                self.func = other.func
                self.args = other.args
                self.kwargs = other.kwargs
                self.done = False
                self.flat = False
            
            return True
        
        return False

    async def execute(self, ctx: Context):
        """Execute main function."""
        if self.flat:
            raise RuntimeError('cannot re-execute a restored stage')

        # initialize state
        self.done = False
        self.version += 1
        self.data = {}

        result = self.func.func(*self.args, **self.kwargs)

        if asyncio.iscoroutine(result):
            result = await result

        self.result = result

        # remove outdated child stages
        self.history = list(filter(lambda s: s.parent_version == self.version, self.history))

        # save execution state
        self.done = True
        asyncio.create_task(ctx.checkpoint())

        return result

    async def progress(self, stage: Stage, ctx: Context):
        """Compare and execute a child step.

        Args:
            stage (Stage): Child stage to be executed.
            ctx (Context): Context of current stage.
            checkpoint (Callable): function to save stage state.

        Returns:
            Any: Return value of stage function.
        """
        for s in self.history:
            if s.renew(stage):
                if not s.done:
                    await create_child_task(s.execute(ctx), s)
                
                s.parent_version = stage.parent_version
                return s.result

        self.history.append(stage)
        await create_child_task(stage.execute(ctx), stage)

        return stage.result


def current_stage() -> Stage | None:
    """Get current running stage."""
    try:
        return asyncio.current_task()._sk_stage # type: ignore

    except:
        return None
