from __future__ import annotations
import asyncio
from typing import Callable, Dict, Tuple, Literal, Collection, Any, TYPE_CHECKING, cast
from math import ceil
from time import time
from datetime import timedelta
from fractions import Fraction
from functools import partial
from inspect import signature

from .directory import Directory
from .config import config
from .lib.job.job import _jobs

if TYPE_CHECKING:
    from .lib.job.job import Job


class InsufficientWalltime(TimeoutError):
    """Timeout due in insufficient walltime."""


class Runner:
    """Class for managing MPI / multiprocessing execution."""
    # pending task, asyncio.Lock -> (nnodes, priority) (Fraction for MPI tasks, int for multiprocessing tasks)
    pending: Dict[asyncio.Lock, Tuple[Fraction | int, int]] = {}

    # running tasks, asyncio.Lock -> nnodes
    running: Dict[asyncio.Lock, Fraction | int] = {}

    # object for cluster configuration
    job: Job = cast(Job, None)

    # loop that checks pending and running tasks every second
    loop: asyncio.Task | None = None

    def dispatch(self, lock: asyncio.Lock, nnodes: Fraction | int) -> bool:
        """Execute a task if resource is available."""
        if self.job is None:
            self.job = _jobs[config['job']['job']](config['job'])

        # use multiprocessing if nnodes is int
        mp = isinstance(nnodes, int)

        if mp:
            # task is executed with multiprocessing
            ntotal = config['job'].get('cpus_per_node') or self.job.cpus_per_node

        else:
            # task is executed with MPI
            ntotal = config['job'].get('nnodes') or 1

        nrunning = sum(v for v in self.running.values() if isinstance(v, int) == mp)

        if nrunning == 0 or nnodes <= ntotal - nrunning:
            self.running[lock] = nnodes
            return True

        return False
    
    async def create_loop(self):
        # run next MPI task
        while len(self.pending) > 0:
            # sort entries by their node number and priority, np is (nnodes, priority)
            nnodes_max = max(np[0] for np in self.pending.values())
            pendings = sorted(self.pending.items(), key=lambda item: item[1][1] * nnodes_max + item[1][0], reverse=True)

            # execute tasks if resource is available
            for lock, np in pendings:
                if self.dispatch(lock, np[0]):
                    del self.pending[lock]
                    lock.release()
            
            await asyncio.sleep(1)


    async def mpiexec(self, root: Directory, cwd: str | None, cmd: str | Callable[[], Any] | Callable[[str], Any],
            nprocs: int, cpus_per_proc: int, gpus_per_proc: int | Tuple[Literal[1], int], multiprocessing: bool,
            custom_mpiexec: str | None, custom_nnodes: int | Tuple[int, int] | None, args: Collection[str] | None, mpiargs: Collection[Any] | None,
            fname: str | None, check_output: Callable[..., None] | None, timeout: Literal['auto'] | float | None, priority: int):
        """Schedule the execution of MPI task."""
        # error occurred
        err = None

        # task queue controller
        lock = asyncio.Lock()

        try:
            # remove unused proceesses
            if mpiargs:
                nprocs = min(len(mpiargs), nprocs)

            # calculate node number
            if multiprocessing:
                nnodes = nprocs

            else:
                nnodes = Fraction(nprocs * cpus_per_proc, self.job.cpus_per_node)

                if isinstance(gpus_per_proc, tuple):
                    # check if GPU is enabled
                    if not self.job.gpus_per_node:
                        raise ValueError('GPU is not enabled in current system')

                    # 1 GPU is shared by <mps> processes
                    if len(gpus_per_proc) != 2 or gpus_per_proc[0] != 1:
                        raise ValueError(f'incorrect mps configuration `{gpus_per_proc}`')

                    mps = gpus_per_proc[1]

                    if nprocs % mps != 0:
                        raise ValueError(f'nprocs must be a multiple of mps ({nprocs}, {mps})')

                    nnodes = max(nnodes, Fraction(nprocs//mps, self.job.gpus_per_node))

                elif gpus_per_proc > 0:
                    nnodes = max(nnodes, Fraction(
                        nprocs * gpus_per_proc, self.job.gpus_per_node))

                if not self.job.share_node:
                    nnodes = Fraction(int(ceil(nnodes)))

            # wait for node resources
            await lock.acquire()

            self.pending[lock] = (nnodes, priority)

            if self.loop is None:
                self.loop = asyncio.create_task(self.create_loop())

            await lock.acquire()

            # determine file name for log, stdout and stderr
            # determine file name for log, stdout and stderr
            if fname is None:
                if isinstance(cmd, str):
                    fname = cmd.split(' ')[0].split('/')[-1].split('.')[-1]

                else:
                    func = cmd

                    while isinstance(func, partial):
                        func = func.func
                    
                    if hasattr(func, '__name__'):
                        return func.__name__.lstrip('_')

                if fname is None:
                    fname = 'mpiexec'

                else:
                    fname = 'mpiexec_' + fname

            if root.has(f'{fname}.log'):
                i = 1

                while root.has(f'{fname}#{i}.log'):
                    i += 1

                fname = f'{fname}#{i}'

            if not callable(cmd):
                args = None
                mpiargs = None

            if callable(cmd) or multiprocessing:
                # save function as pickle to run in parallel
                if args:
                    args = list(args)

                if mpiargs:
                    _args = sorted(mpiargs)
                    mpiargs = []
                    chunk = int(ceil(len(_args) / nprocs))

                    for i in range(nprocs - 1):
                        mpiargs.append(_args[i * chunk: (i + 1) * chunk])

                    mpiargs.append(_args[(nprocs - 1) * chunk:])

                root.rm(f'{fname}.*')
                root.dump((cmd, args, mpiargs, cwd), f'{fname}.pickle')
                cmd = f'python -m "stagekit.mpi" {fname}'
                cwd = None

            # wrap with parallel execution command
            if multiprocessing:
                cmd = f'{cmd} -mp {nprocs}'

            else:
                cmd = self.job.mpiexec(cmd, nprocs, cpus_per_proc, gpus_per_proc)

            # write the command actually used
            root.write(f'{cmd}\n', f'{fname}.log')
            time_start = time()

            # timeout due to insufficient walltime
            timeout_walltime = False

            # create subprocess to execute task
            with open(root.path(f'{fname}.stdout'), 'w') as f_o, open(root.path(f'{fname}.stderr'), 'w') as f_e:

                # execute in subprocess
                process = await asyncio.create_subprocess_shell(cmd, cwd=cwd, stdout=f_o, stderr=f_e)

                if timeout == 'auto':
                    if self.job.timeout:
                        timeout = self.job.remaining * 60
                        timeout_walltime = True

                    else:
                        timeout = None

                if timeout:
                    try:
                        await asyncio.wait_for(process.communicate(), timeout)

                    except asyncio.TimeoutError as e:
                        if timeout_walltime:
                            raise InsufficientWalltime('Insufficient walltime.')

                        else:
                            raise TimeoutError('insufficient execution time')

                else:
                    await process.communicate()

            # custom function to resolve output
            if check_output:
                nargs = len(signature(check_output).parameters)

                if nargs == 0:
                    check_output()

                elif nargs == 1:
                    check_output(root.read(f'{fname}.stdout'))

                else:
                    check_output(root.read(f'{fname}.stdout'), root.read(f'{fname}.stderr'))

            # write elapsed time
            root.write(f'\nelapsed: {timedelta(seconds=int(time()-time_start))}\n', f'{fname}.log', 'a')

            if root.has(f'{fname}.error'):
                raise RuntimeError(root.read(f'{fname}.error'))

            elif process.returncode:
                raise RuntimeError(f'{cmd}\nexit code: {process.returncode}')

        except Exception as e:
            err = e

        # clear entry
        if lock in self.pending:
            del self.pending[lock]

        if lock in self.running:
            del self.running[lock]

        if err:
            raise err

        return fname
