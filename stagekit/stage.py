from __future__ import annotations
from typing import Any, List, Mapping, Iterable, Callable, Tuple, TYPE_CHECKING
import asyncio

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

    # error occured during execution
    error: Exception | None = None

    def __init__(self, func: StageFunc, args: Iterable[Any], kwargs: Mapping[str, Any], cwd: str | None):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.cwd = cwd

        self.history = []
        self.scheduled = []
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
        self.step = 0
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
        if self.step < len(self.history):
            # skip if stage is already created or executed
            s = self.history[self.step]

            if s == stage:
                if not s.done:
                    # FIXME: use asyncio.current_task() to determine context
                    # await -> asyncio.create_task()
                    await s.execute(ctx, checkpoint)

                self.step += 1
                return s.result

        self.history = self.history[:self.step]
        self.history.append(stage)
        await stage.execute(ctx, checkpoint)
        self.step += 1

        return stage.result
