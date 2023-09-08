from __future__ import annotations
from typing import TYPE_CHECKING
from .io import IO, define_io

if TYPE_CHECKING:
    import numpy as np


class Numpy(IO):
    def __init__(self):
        from numpy import load, save

        self._load = load
        self._save = save

    def load(self, src: str):
        return self._load(src)


    def dump(self, obj: np.ndarray, dst: str):
        self._save(dst, obj)


define_io('npy', Numpy)
