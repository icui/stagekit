from __future__ import annotations
from os.path import dirname, basename, splitext
from os import getcwd
from sys import path
from importlib import import_module
import __main__

from .data import define_data


# directory of __main__
if hasattr(__main__, '__file__'):
    _cwd = dirname(__main__.__file__)

else:
    _cwd = getcwd()


def test(func):
    return callable(func) and hasattr(func, '__module__') and hasattr(func, '__name__')


class Function:
    """Wrapper for Python functions replacing __main__ with absolute path for pickle."""
    # module name of the function
    module: str

    # function name
    name: str

    # path required to import the function
    path: str

    def __init__(self, func):
        if func.__module__ == '__main__':
            self.module = splitext(basename(__main__.__file__))[0]
        
        else:
            self.module = func.__module__

        self.name = func.__name__
        self.path = _cwd
    
    def __eq__(self, other):
        if isinstance(other, Function):
            return self.module == other.module and self.name == other.name
        
        return False
    
    def load(self):
        if self.path not in path:
            path.insert(1, self.path)

        return getattr(import_module(self.module), self.name)


define_data(test, Function)
