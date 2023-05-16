from .system import System


class Local(System):
    """Run tasks locally."""
    def submit(self):
        pass

    def mpiexec(self):
        pass

    def write(self):
        pass
