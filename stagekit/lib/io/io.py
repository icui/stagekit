from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Type, Collection
from sys import stderr


# dict of file extensions -> reader/writer classes
_io_cls: Dict[str, Type[IO]] = {}

# dict of file extensions -> reader/writer objects
_io_obj: Dict[str, IO] = {}


class IO(ABC):
    """Base class for defining ctx.load() and ctx.dump()."""
    @abstractmethod
    def load(self, src: str):
        """Load a file."""

    def dump(self, obj: object, dst: str):
        """Save a file (not a abstract method because dump() is optional for some file types)."""
        raise NotImplementedError(f'dump() is not implemented for `{dst}`')


def define_io(ext: str | Collection[str], obj: Type[IO]):
    """Define a file reader / writer."""
    if isinstance(ext, str):
        _io_cls[ext] = obj
    
    else:
        for e in ext:
            if e in _io_cls:
                print(f'warning: redefining reader / writer for file extension {e}', file=stderr)
            
            _io_cls[e] = obj

def get_io(ext: str) -> IO:
    """Get reader / writer for a file extension."""
    if ext not in _io_obj:
        if ext in _io_cls:
            _io_obj[ext] = _io_cls[ext]()
    
        else:
            raise NotImplementedError(f'reader / writer not implemented for file extension {ext}')
    
    return _io_obj[ext]
