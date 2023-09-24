from __future__ import annotations
from os.path import dirname, basename, splitext
from importlib import import_module
import __main__

from .data import define_data


def test(func):
    return callable(func) and hasattr(func, '__module__') and hasattr(func, '__name__')


# extra paths to be imported for functions
inserted_paths = {}


class Function:
    """Wrapper for Python functions replacing __main__ with absolute path for pickle."""
    # module name of the function
    module: str

    # function name
    name: str

    # extra import path required
    path: str | None = None

    def __init__(self, func):
        self.name = func.__name__

        if func.__module__ == '__main__':
            self.module = splitext(basename(__main__.__file__))[0]

            if hasattr(__main__, '__file__'):
                self.path = dirname(__main__.__file__)
        
        else:
            self.module = func.__module__

            if self.module in inserted_paths:
                self.path = inserted_paths[self.module]

    def __eq__(self, other):
        if isinstance(other, Function):
            return self.module == other.module and self.name == other.name and self.path == other.path
        
        return False
    
    def load(self):
        from sys import path

        if self.path and self.path not in path:
            path.insert(1, self.path)
            inserted_paths[self.module] = self.path

        return getattr(import_module(self.module), self.name)


define_data(test, Function)
