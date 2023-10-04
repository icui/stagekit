from __future__ import annotations
from typing import Collection, TYPE_CHECKING

if TYPE_CHECKING:
    from mpi4py.MPI import Intracomm


class Stat:
    """Status accessed by subprocesses."""
    # Index of current MPI process
    rank: int = 0

    # Total number of MPI processes
    size: int = 0

    # MPI Comm World
    comm: Intracomm | None = None

    # mpiargs for current rank
    mpiargs: Collection | None = None

    # currently running in subprocess
    in_subprocess = False


# MPI info accessed by processes
stat = Stat()
