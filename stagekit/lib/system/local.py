from .system import System


class Local(System):
    """Run tasks locally."""
    submit = 'python'

    def mpiexec(self):
        pass

    def write(self, main: str):
        return main


sk_system = Local
