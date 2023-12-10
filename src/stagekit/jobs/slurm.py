from .job import Job, define_job
from os import environ


class Slurm(Job):
    """Run tasks locally."""
    @property
    def jobid(self) -> str:
        return environ['SLURM_JOB_ID']

    def mpiexec(self, cmd, nprocs=1, cpus_per_proc=1, gpus_per_proc=0):
        return f'srun -n {nprocs} --cpus-per-task {cpus_per_proc} --gpus-per-task {gpus_per_proc} {cmd}'

    def isrunning(self, jobid: str) -> bool:
        return super().isrunning(jobid)


define_job('slurm', Slurm)
