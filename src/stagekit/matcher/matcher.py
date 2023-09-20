from __future__ import annotations
from abc import ABC
from typing import Type, Dict, Callable
from collections.abc import Collection
from sys import stderr


# dict of variable test function -> matcher class
_matcher_cls: Dict[Callable[..., bool], Type[Matcher]] = {}


class Matcher(ABC):
    """Base class for mapping stage function arguments to a type that supports __eq__."""
    def __init__(self, _):
        pass


def define_matcher(test: Callable[..., bool], obj: Type[Matcher]):
    """Define a file reader / writer."""
    _matcher_cls[test] = obj
