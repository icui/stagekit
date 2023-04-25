from __future__ import annotations
from typing import Any, List, Mapping, Iterable, Callable, Tuple, TYPE_CHECKING
import asyncio

if TYPE_CHECKING:
    from .main import StageFunc

# typing for Stage configuration
# [0]: wrapped function
# [1]: arguments passed to self.func
# [2]: keyword argumentd passed to self.func
StageConfig = Tuple['StageFunc', Iterable[Any], Mapping[str, Any]]


class Stage:
    """Wrapper of a function to save execution progress."""
    # (static property) stage currently being executed
    current: Stage | None = None

    # (both static and non-static property) data defined by stage function accessed through ctx
    # use static property Stage.data as top level of inheritance
    data: dict = {}

    # stage function and arguments
    config: StageConfig

    # index of current child stage
    step: int = 0

    # executed child stages
    history: List[Stage]

    # MPI or subprocess function calls awaiting execution after stage function finishes
    scheduled: List[Callable]

    # parent stage
    parent: Stage | None = None

    # return value of self.func
    result: Any = None

    # main function successfully executed
    done = False

    # error occured during execution
    error: Exception | None = None

    def __init__(self, config: StageConfig):
        self.config = config
        self.history = []
        self.scheduled = []
        self.data = {}

    async def execute(self):
        """Execute main function."""
        # change context to self
        self.parent = Stage.current
        Stage.current = self

        # initialize state
        self.step = 0
        self.done = False
        self.scheduled.clear()

        result = self.config[0].func(*self.config[1], **self.config[2])
        if asyncio.iscoroutine(result):
            result = await result
        
        self.result = result
        self.done = True

        # restore context to parent stage
        Stage.current = self.parent

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
