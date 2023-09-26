from __future__ import annotations
from typing import TYPE_CHECKING

from .data import define_data, Data

if TYPE_CHECKING:
    import numpy as np


def test(arr):
    import numpy as np

    return isinstance(arr, np.ndarray)


class Numpy(Data):
    """Wrapper for numpy arrays."""
    data: np.ndarray
    
    @property
    def size(self):
        return self.data.nbytes

    def __eq__(self, other: Numpy):
        if not isinstance(other, Numpy):
            return False

        return all(self.data == other.data)


define_data(test, Numpy)
