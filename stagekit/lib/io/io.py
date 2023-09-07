from abc import ABC, abstractmethod


class IO(ABC):
    """Base class for defining ctx.load() and ctx.dump()."""
    @abstractmethod
    def load():
        """Load a file."""

    def dump(self, obj: object, dst: str):
        """Save a file (not a abstract method because dump() is optional for some file types)."""
        raise NotImplementedError(f'dump() is not implemented for `{dst}`')
