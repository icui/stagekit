from __future__ import annotations

from .function import Function, define_data


def test(obj):
    return obj.__getstate__() is not None


class Object:
    """Wrapper for objects replacing __main__ with absolute path for pickle."""
    # object constructor
    func: Function

    # object state
    state: str

    def __init__(self, obj):
        self.func = Function(obj.__class__)
        self.state = obj.__getstate__()

    def __eq__(self, other):
        if isinstance(other, Object):
            return self.func == other.func and self.state == other.state

        return self == Object(other)


define_data(test, Object)
