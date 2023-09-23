from __future__ import annotations
from os.path import dirname, basename, splitext
from importlib import import_module
import __main__

from .data import define_data


def test(func):
    return callable(func) and hasattr(func, '__module__') and hasattr(func, '__name__')


# extra paths to be imported for functions
inserted_paths = []


class Function:
    """Wrapper for Python functions replacing __main__ with absolute path for pickle."""
    # module name of the function
    module: str

    # function name
    name: str

    def __init__(self, func):
        self.name = func.__name__

        if func.__module__ == '__main__':
            self.module = splitext(basename(__main__.__file__))[0]

            if hasattr(__main__, '__file__'):
                src = dirname(__main__.__file__)

                if src not in inserted_paths:
                    from stagekit import ws
                    inserted_paths.append(src)
                    ws.dump(inserted_paths, 'paths.json')
        
        else:
            self.module = func.__module__

    def __getstate__(self) -> object:
        return {'m': self.module, 'n': self.name}

    def __setstate__(self, state):
        self.module = state['m']
        self.name = state['n']

    def __eq__(self, other):
        if isinstance(other, Function):
            return self.module == other.module and self.name == other.name
        
        return False
    
    def load(self):
        return getattr(import_module(self.module), self.name)


define_data(test, Function)
