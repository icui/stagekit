from .job import Job, define_job


class Slurm(Job):
    """Run tasks locally."""
    def mpiexec(self, cmd, nprocs=1, cpus_per_proc=1, gpus_per_proc=0):
        return f'srun -n {nprocs} --cpus-per-task {cpus_per_proc} --gpus-per-task {gpus_per_proc} {cmd}'


define_job('slurm', Slurm)
