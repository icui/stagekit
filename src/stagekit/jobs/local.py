from .job import Job, define_job
from os import cpu_count


class Local(Job):
    """Run tasks locally."""
    nnodes = cpu_count() or 1
    no_mpi = True

    def mpiexec(self, cmd, nprocs=1, cpus_per_proc=1, gpus_per_proc=0):
        raise RuntimeError('mpiexec should not be called when no_mpi flag is onb')

    def isrunning(self, jobid: str):
        return False


define_job('local', Local)
