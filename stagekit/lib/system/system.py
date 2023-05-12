from typing import Callable


class System:
    """Base class for a job scheduler configuration."""
    # whether a node can be shared by multiple MPI calls
    share_node: bool

    # whether a gpu can be shared by multiple MPI processes (multi-process service)
    share_gpu: bool

    # number of cpus per node
    cpus_per_node: int

    # number of gpus per node
    gpus_per_node: int

    # command to submit a job script
    submit: str

    # command to submit a job script
    mpiexec: Callable

    # command to submit a job script
    write: Callable
