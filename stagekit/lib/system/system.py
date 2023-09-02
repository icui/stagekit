from typing import Callable, Tuple, Literal, Iterable, Any
from stagekit import ctx
from abc import ABC, abstractmethod


class System(ABC):
    """Base class for a job scheduler configuration."""
    # whether a node can be shared by multiple MPI calls
    share_node: bool

    # whether a gpu can be shared by multiple MPI processes (multi-process service)
    share_gpu: bool

    # number of cpus per node
    cpus_per_node: int = 1

    # number of gpus per node, use (1, n) if a GPU is shared by n processes
    gpus_per_node: int | Tuple[Literal[1], int] = 1

    # command to submit a job script
    submit: str

    @abstractmethod
    def mpiexec(self, cmd: str | Callable[[], Any] | Callable[[str], Any],
        nprocs: int = 1, cpus_per_proc: int = 1, gpus_per_proc: int | Tuple[int, int] = 0, *,
        multiprocessing: bool = False, custom_mpiexec: str | None = None, custom_nnodes: int | Tuple[int, int] | None = None,
        args: Iterable[str] | None = None, group_args: bool = False,
        fname: str | None = None, check_output: Callable[..., None] | None,
        timeout: Literal['auto'] | float | None, ontimeout: Literal['raise'] | Callable[[], None] | None,
        priority: int | None = None):
        """Command to call MPI or multiprocessing functions or shell commands."""

    @abstractmethod
    def write(self, main: str) -> str:
        """Write job submission script.

        Args:
            main (str): Python file that defines and executes the main workflow.

        Returns:
            str: File name of the job submission script.
        """
