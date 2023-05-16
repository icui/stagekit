from __future__ import annotations
from typing import Any, List, Dict, Mapping, Iterable, TYPE_CHECKING
import asyncio

from .task import create_task
from .config import config

if TYPE_CHECKING:
    from .wrapper import StageFunc
    from .context import Context


class Stage:
    """Wrapper of a function to save execution progress.
        Note: Stage is intended to be a purely internal class,
        do not create a stage directly with Stage(), use decorator @stage instead."""
    # (both static and non-static property) data defined by stage function accessed through ctx
    # use static property Stage.data as root of inheritance
    data: dict = config['data']

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

    # re-running existing stage
    rerun = False

    # error occured during execution
    error: Exception | None = None

    # number of times executed
    version: int = 0

    # number of times parent stage is executed
    parent_version: int

    def __init__(self, func: StageFunc, args: Iterable[Any], kwargs: Mapping[str, Any], cwd: str | None, parent_version: int):
        self.func = func
        self.args = list(args)
        self.kwargs = dict(kwargs)
        self.cwd = cwd
        self.parent_version = parent_version

        self.history = []
        self.data = {}

    def __eq__(self, stage: Stage | None):
        if isinstance(stage, Stage):
            if self.func != stage.func:
                return False
        
            if self.cwd != stage.cwd:
                return False

            if len(self.args) != len(stage.args):
                return False

            if len(self.kwargs) != len(stage.kwargs):
                return False

            skips = set()

            if isinstance(self.func.skip, str):
                skips.add(self.func.skip)
            
            elif self.func.skip:
                skips.update(self.func.skip)
            
            co_varnames = self.func.func.__code__.co_varnames

            for i in range(len(self.args)):
                if co_varnames[i] in skips:
                    continue

                if self.args[i] != stage.args[i]:
                    return False
            
            for k in self.kwargs:
                if k in skips:
                    continue

                if self.kwargs[k] != stage.kwargs[k]:
                    return False

            return True

        return False

    async def execute(self, ctx: Context):
        """Execute main function."""
        # initialize state
        self.done = False
        self.version += 1

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
            if s == stage:
                if not s.done or stage.func.rerun == True or (stage.func.rerun == 'auto' and len(s.history) > 0):
                    # re-run existing stage if:
                    # (1) stage not completed
                    # (2) stage is set to alwarys re-run
                    # (3) stage is set to auto re-run and stage has child stage
                    s.func = stage.func
                    s.args = stage.args
                    s.kwargs = stage.kwargs
                    s.rerun = s.done
                    s.done = False
                    await create_task(s.execute(ctx), s)
                
                s.parent_version = stage.parent_version
                return s.result

        self.history.append(stage)
        await create_task(stage.execute(ctx), stage)

        return stage.result


def current_stage() -> Stage | None:
    """Get current running stage."""
    try:
        return asyncio.current_task()._sk_stage # type: ignore

    except:
        return None
