from __future__ import annotations
from typing import Collection, TYPE_CHECKING
import asyncio
from os import path
from sys import argv, stderr
from traceback import format_exc
from functools import partial
import pickle

if TYPE_CHECKING:
    from mpi4py.MPI import Intracomm


class Stat:

    # Index of current MPI process
    rank: int = 0

    # Total number of MPI processes
    size: int = 0

    # MPI Comm World
    comm: Intracomm | None = None

    # mpiargs for current rank
    mpiargs: Collection | None = None


# MPI info accessed by processes
stat = Stat()


def _call(size: int, idx: int):
    from stagekit.mpi import stat

    mpidir = path.dirname(argv[1]) or '.'

    if size == 0:
        # use mpi
        from mpi4py.MPI import COMM_WORLD as comm

        stat.comm = comm
        stat.rank = comm.Get_rank()
        stat.size = comm.Get_size()

    else:
        # use multiprocessing
        stat.rank = idx
        stat.size = size
    
    # saved function and arguments from main process
    with open(f'{argv[1]}.pickle', 'rb') as f:
        func, args, mpiargs = pickle.load(f)
    

    # call target function
    if callable(func):
        if mpiargs:
            stat.mpiargs = mpiargs[stat.rank]
        
        if asyncio.iscoroutine(result := func(*args)):
            asyncio.run(result)
    
    else:
        from subprocess import check_call
        check_call(func, shell=True, cwd=mpidir)


if __name__ == '__main__':
    try:
        if len(argv) > 3 and argv[2] == '-mp':
            # use multiprocessing
            np = int(argv[3])

            if np == 1:
                _call(np, 0)
            
            else:
                from multiprocessing import Pool

                with Pool(processes=np) as pool:
                    pool.map(partial(_call, np), range(np))
        
        else:
            # use mpi
            _call(0, 0)
    
    except Exception:
        err = format_exc()
        print(err, file=stderr)

        with open(f'{argv[1]}.error', 'a') as f:
            f.write(err)
