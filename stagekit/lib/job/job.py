from __future__ import annotations
from typing import Tuple, Literal
from abc import ABC, abstractmethod
from typing import Dict, Type
from time import time


# dict containing system names and modules
_jobs: Dict[str, Type[Job]] = {}


class Job(ABC):
    """Base class for job scheduler configuration."""
    # whether system has an additional head node
    head_node: bool = True

    # whether a node can be shared by multiple MPI calls
    share_node: bool = True

    # whether a gpu can be shared by multiple MPI processes (multi-process service)
    share_gpu: bool = False

    # number of cpus per node
    cpus_per_node: int

    # number of gpus per node
    gpus_per_node: int

    # time interval to check status updates from other jobs (in minutes)
    status_update: int | float = 0.1

    # job name
    name: str

    # time requested for the job (in minutes)
    walltime: int | float

    # number of nodes to request and run MPI tasks, defaults to 1
    nnodes: int

    # max number of jobs to execute the workflow
    njobs: int = 1

    # number of jobs to submit as a job array
    array: int = 1

    # exit early to avoid being killed abruptly (in minutes)
    gap: int | float = 2.0

    # start time of execution
    _exec_start: float


    @property
    def timeout(self) -> bool:
        """Whether task will be killed after walltime runs out."""
        return False

    @property
    def remaining(self) -> float:
        """Remaining walltime in minutes."""
        return self.walltime - self.gap - (time() - self._exec_start) / 60
    
    def __init__(self, config: dict):
        # set job attributes
        required_keys = ['nnodes', 'walltime', 'cpus_per_node', 'gpus_per_node']

        for key in required_keys:
            if key not in config and not hasattr(self, key):
                raise KeyError(f'required job config `{key}` is missing')

        for key, val in config.items():
            if key != 'job':
                setattr(self, key, val)
        
        # execution start time
        self._exec_start = time()

    @abstractmethod
    def mpiexec(self, cmd: str, nprocs: int = 1, cpus_per_proc: int = 1, gpus_per_proc: int | Tuple[Literal[1], int] = 0) -> str:
        """Command to call MPI or multiprocessing functions or shell commands."""

    def write(self, cmd: str):
        """Write job submission script for a command.

        Args:
            cmd (str): Command to be submitted as a job.
            job (Job): Job configuration.

        """


def define_job(name: str, cls: Type[Job]):
    """Define a job scheduler system."""
    if name:
        print(f'warning: redefining system `{name}`')

    _jobs[name] = cls
