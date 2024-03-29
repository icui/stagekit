from __future__ import annotations
import asyncio
from typing import Callable, Dict, Tuple, List, Literal, Collection, cast
from math import ceil
from time import time
from datetime import timedelta
from fractions import Fraction
from inspect import signature
from sys import stderr

from .directory import ws
from .config import config
from .wrapper import stage
from .data.function import Function
from .jobs.job import Job, _job_cls


class InsufficientWalltime(TimeoutError):
    """Timeout due in insufficient walltime."""


class MPIOutput:
    """Return value of mpiexec."""
    # file name of the output files
    fname: str | None

    # cache of log
    _log: str | None = None

    # cache of stdout
    _stdout: str | None = None

    # cache of stderr
    _stderr: str | None = None

    def __init__(self, fname: str | None):
        self.fname = fname

    @property
    def log(self) -> str | None:
        """Log content of the execution."""
        if self._log is None and self.fname:
            self._log = ws.read(self.fname + '.log')
        
        return self._log

    @property
    def stdout(self) -> str | None:
        """stdout of the execution."""
        if self._stdout is None and self.fname:
            self._stdout = ws.read(self.fname + '.stdout')
        
        return self._stdout

    @property
    def stderr(self) -> str | None:
        """stderr of the execution."""
        if self._stderr is None and self.fname:
            self._stderr = ws.read(self.fname + '.stderr')
        
        return self._stderr


# pending task, asyncio.Lock -> (nnodes, priority) (Fraction for MPI tasks, int for multiprocessing tasks)
_pending: Dict[asyncio.Lock, Tuple[Fraction | int, int]] = {}

# running tasks, asyncio.Lock -> nnodes
_running: Dict[asyncio.Lock, Fraction | int] = {}

# object for cluster configuration
_job: Job = cast(Job, None)

# loop that checks pending and running tasks every second
_task: asyncio.Task | None = None

# workers from other jobs or processes that can execute tasks
_workers: Dict[str, List[asyncio.Lock]] = {}


def _dispatch(lock: asyncio.Lock, nnodes: Fraction | int) -> bool:
    """Execute a task if resource is available."""
    # use multiprocessing if nnodes is int
    mp = isinstance(nnodes, int)

    if mp:
        # task is executed with multiprocessing
        ntotal = _job.cpus_per_node

    else:
        # task is executed with MPI
        ntotal = _job.nnodes

    nrunning = sum(v for v in _running.values() if isinstance(v, int) == mp)

    if nrunning == 0 or nnodes <= ntotal - nrunning:
        _running[lock] = nnodes
        return True

    return False


async def _loop():
    global _task

    # run next MPI task
    while len(_pending) > 0:
        # sort entries by their node number and priority, np is (nnodes, priority)
        nnodes_max = max(np[0] for np in _pending.values())
        pendings = sorted(_pending.items(), key=lambda item: item[1][1] * nnodes_max + item[1][0], reverse=True)

        # execute tasks if resource is available
        for lock, np in pendings:
            if _dispatch(lock, np[0]):
                del _pending[lock]
                lock.release()
        
        # send task to external jobs if any external job is active
        for job in ws.ls('jobs'):
            starttime, duration, nnodes = ws.read(f'jobs/{job}').split(',')
            if float(starttime) + float(duration) < time():
                ws.rm(f'jobs/{job}')

            else:
                pass

        await asyncio.sleep(1)

    _task = None


@stage(argmap={'check_output': None})
async def mpiexec(cmd: str | Callable,
            nprocs: int = 1, cpus_per_proc: int = 1, gpus_per_proc: int | Tuple[Literal[1], int] = 0, *, cwd: str | None = None,
            multiprocessing: bool = False, custom_exec: str | None = None, custom_nnodes: int | Tuple[int, int] | None = None,
            args: Collection | None = None, mpiargs: Collection | None = None, fname: str | None = None,
            check_output: Callable[..., None] | None = None, timeout: Literal['auto'] | float | None = 'auto',
            priority: int = 0) -> MPIOutput:
    """Schedule the execution of MPI task."""
    global _job
    global _task

    if _job is None:
        _job = _job_cls[config['job']['job']](config['job'])

    # remove unused proceesses
    if mpiargs:
        nprocs = min(len(mpiargs), nprocs)

    # calculate node number
    multiprocessing = _job.no_mpi or multiprocessing

    if custom_exec and custom_nnodes:
        if isinstance(custom_nnodes, tuple):
            if multiprocessing:
                nnodes = int(ceil(custom_nnodes[0] / custom_nnodes[1]))
            
            else:
                nnodes = Fraction(custom_nnodes[0], custom_nnodes[1])

        else:
            if multiprocessing:
                nnodes = custom_nnodes
            
            else:
                nnodes = Fraction(custom_nnodes)

    elif multiprocessing:
        nnodes = nprocs

    else:
        nnodes = Fraction(nprocs * cpus_per_proc, _job.cpus_per_node)

        if isinstance(gpus_per_proc, tuple):
            # check if GPU is enabled
            if not _job.gpus_per_node:
                raise ValueError('GPU is not enabled in current system')

            # 1 GPU is shared by <mps> processes
            if len(gpus_per_proc) != 2 or gpus_per_proc[0] != 1:
                raise ValueError(f'incorrect mps configuration `{gpus_per_proc}`')

            mps = gpus_per_proc[1]

            if nprocs % mps != 0:
                raise ValueError(f'nprocs must be a multiple of mps ({nprocs}, {mps})')

            nnodes = max(nnodes, Fraction(nprocs // mps, _job.gpus_per_node))

        elif gpus_per_proc > 0:
            nnodes = max(nnodes, Fraction(nprocs * gpus_per_proc, _job.gpus_per_node))

        if not _job.share_node:
            nnodes = Fraction(int(ceil(nnodes)))

    # error occurred
    err = None

    # task queue controller
    lock = asyncio.Lock()

    try:
        # wait for node resources
        await lock.acquire()

        _pending[lock] = (nnodes, priority)

        if _task is None:
            _task = asyncio.create_task(_loop())

        await lock.acquire()

        # determine file name for log, stdout and stderr
        if fname is None:
            if isinstance(cmd, str):
                fname = cmd.split(' ')[0].split('/')[-1].split('.')[0]

            elif hasattr(cmd, '__name__'):
                    fname = cmd.__name__.lstrip('_')

            if fname is None:
                fname = 'mpiexec'

            else:
                fname = 'mpiexec_' + fname

        if ws.has(f'{fname}.log'):
            i = 1

            while ws.has(f'{fname}#{i}.log'):
                i += 1

            fname = f'{fname}#{i}'

        if not callable(cmd):
            if args or mpiargs:
                print('warning: args / mpiargs are ignored', file=stderr)

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

            ws.rm(f'{fname}.*')

            if callable(cmd):
                cmd = Function(cmd) # type: ignore

            ws.dump((cmd, args, mpiargs), f'{fname}.pickle')

            cmd = f'python -m "stagekit.subprocess.exec" {ws.path()} {fname}'
            cwd = None

        # wrap with parallel execution command
        if custom_exec:
            cmd = f'{custom_exec} {cmd}'

        elif multiprocessing:
            cmd = f'{cmd} -mp {nprocs}'

        else:
            cmd = _job.mpiexec(cmd, nprocs, cpus_per_proc, gpus_per_proc)

        # write the command actually used
        ws.write(f'{cmd}\n', f'{fname}.log')
        time_start = time()

        # timeout due to insufficient walltime
        timeout_walltime = False

        # create subprocess to execute task
        with open(ws.path(f'{fname}.stdout'), 'w') as f_o, open(ws.path(f'{fname}.stderr'), 'w') as f_e:
            # execute in subprocess
            process = await asyncio.create_subprocess_shell(cmd, cwd=cwd, stdout=f_o, stderr=f_e)

            if timeout == 'auto':
                if _job.time_limited:
                    timeout = _job.remaining * 60
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
                check_output(ws.read(f'{fname}.stdout'))

            else:
                check_output(ws.read(f'{fname}.stdout'), ws.read(f'{fname}.stderr'))

        # write elapsed time
        ws.write(f'\nelapsed: {timedelta(seconds=int(time()-time_start))}\n', f'{fname}.log', 'a')

        if ws.has(f'{fname}.error'):
            raise RuntimeError(ws.read(f'{fname}.error'))

        elif process.returncode:
            raise RuntimeError(f'{cmd}\nexit code: {process.returncode}')

    except Exception as e:
        err = e

    # clear entry
    if lock in _pending:
        del _pending[lock]

    if lock in _running:
        del _running[lock]

    if err:
        raise err

    return MPIOutput(fname)
