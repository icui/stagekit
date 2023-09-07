from .job import Job, define_job


class Local(Job):
    """Run tasks locally."""
    def mpiexec(self):
        pass


define_job('local', Local)
