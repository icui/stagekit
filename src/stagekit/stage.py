from __future__ import annotations
from typing import Any, List, Dict, Mapping, Collection, TYPE_CHECKING
import asyncio

from .task import create_child_task
from .data.data import _data_cls
from .data.function import Function

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
    
        state['flat'] = True
        state['func'] = self.flatfunc()
        
        args = state['args'] = []
        kwargs = state['kwargs'] = {}

        co_varnames = self.func.func.__code__.co_varnames

        for i, a in enumerate(self.args):
            args.append(self.flatarg(co_varnames[i], a))
        
        for k, a in self.kwargs.items():
            kwargs[k] = self.flatarg(k, a)

        return state

    def __eq__(self, other):
        if not isinstance(other, Stage) or self.flatfunc() != other.flatfunc() or self.cwd != other.cwd:
            return False
        
        if self.flat and other.flat:
            return self.args == other.args and self.kwargs == other.kwargs

        if len(self.args) != len(other.args) or len(self.kwargs) != len(other.kwargs):
            return False

        # get variable names from the non-flat Stage object
        co_varnames = (other if self.flat else self).func.func.__code__.co_varnames

        for i, (arg1, arg2) in enumerate(zip(self.args, other.args)):
            k = co_varnames[i]

            if self.flatarg(k, arg1) != other.flatarg(k, arg2):
                return False
        
        for k in self.kwargs:
            if k not in other.kwargs:
                return False
            
            if self.flatarg(k, self.kwargs[k]) != other.flatarg(k, other.kwargs[k]):
                return False

        return True
    
    def __repr__(self):
        msg = ''

        func = self.func.load() if self.flat else self.func # type: ignore

        if func.name:
            d = self.kwargs.copy()

            co_varnames = func.func.__code__.co_varnames

            for i in range(len(self.args)):
                d[co_varnames[i]] = self.args[i]
            
            msg += func.name(d)

        elif hasattr(func.func, '__name__'):
            msg += func.func.__name__
        
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
    
    def flatfunc(self):
        """Get Function object corresponding to self.func for comparison and serialization."""
        if self.flat:
            return self.func

        return Function(self.func.func)

    def flatarg(self, k: str, val: Any):
        """Flatten an argument of stage function based on argmap parameter or data wrapper.
            Arguments:
                k (str): Argument name.
                val (Any): Argument value.
        """
        if self.flat:
            return val

        argmap = self.func.argmap

        if k in argmap:
            if argmap[k] is None:
                return None

            return argmap[k](val) # type: ignore

        for test, data in _data_cls.items():
            if test(val):
                return data(val)
        
        return val

    def renew(self, other: Stage):
        """Compare self (previously saved stage) with a new stage and update args if needs to re-run."""
        if other.flat:
            return False

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

        chdir = ctx._chdir
        ctx._chdir = None

        # main function
        result = self.func.func(*self.args, **self.kwargs)

        if asyncio.iscoroutine(result):
            result = await result

        self.result = result

        # remove outdated child stages
        self.history = list(filter(lambda s: s.parent_version == self.version, self.history))

        # save execution state
        self.done = True
        asyncio.create_task(ctx.checkpoint())

        ctx._chdir = chdir

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
