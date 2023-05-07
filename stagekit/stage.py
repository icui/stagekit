from __future__ import annotations
from typing import Any, List, Mapping, Iterable, Callable, Set, TYPE_CHECKING
import asyncio
from .task import create_task


if TYPE_CHECKING:
    from .wrapper import StageFunc
    from .context import Context


class Stage:
    """Wrapper of a function to save execution progress.
        Note: Stage is intended to be a purely internal class,
        do not create a stage directly with Stage(), use decorator @stage instead."""
    # (both static and non-static property) data defined by stage function accessed through ctx
    # use static property Stage.data as root of inheritance
    data: dict = {}

    # stage function
    func: StageFunc

    # arguments of self.func
    args: Iterable[Any]

    # keyword arguments of self.func
    kwargs: Mapping[str, Any]

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

    def __init__(self, func: StageFunc, args: Iterable[Any], kwargs: Mapping[str, Any], cwd: str | None):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.cwd = cwd

        self.history = []
        self.data = {}

    def __eq__(self, stage: Stage | None):
        if stage:
            return self.func == stage.func and \
                self.args == stage.args and \
                self.kwargs == stage.kwargs and \
                self.cwd == stage.cwd

        return False

    async def execute(self, ctx: Context, checkpoint: Callable):
        """Execute main function."""
        # initialize state
        self.done = False
        ctx.goto()

        result = self.func.func(*self.args, **self.kwargs)
        if asyncio.iscoroutine(result):
            result = await result

        self.result = result
        self.done = True
        ctx.goto()

        # save execution state
        asyncio.create_task(checkpoint())

        return result

    async def progress(self, stage: Stage, ctx: Context, checkpoint: Callable):
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
                    await create_task(s.execute(ctx, checkpoint), s)
                
                return s.result

        self.history.append(stage)
        await create_task(stage.execute(ctx, checkpoint), stage)

        return stage.result
