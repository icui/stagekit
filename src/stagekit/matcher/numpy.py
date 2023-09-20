from __future__ import annotations
from typing import TYPE_CHECKING
from os.path import join

from .matcher import define_matcher, Matcher
from stagekit import root
from stagekit.config import config, PATH_WORKSPACE

if TYPE_CHECKING:
    import numpy as np


def test(arr):
    import numpy as np

    return isinstance(arr, np.ndarray)


class Numpy(Matcher):
    """Matcher for numpy arrays."""
    data: np.ndarray | str

    def __init__(self, arr: np.ndarray):
        self.data = arr
    
    def __eq__(self, other: Numpy):
        if not isinstance(other, Numpy):
            return False

        if isinstance(self.data, str):
            import numpy as np

            return all(np.load(self.data) == other.data)

        return all(self.data == other.data)
    
    def __getstate__(self):
        n = config['save_array_larger_than_in_mb']

        if not isinstance(self.data, str) and n and n < self.data.nbytes * 1024 ** 2:
            # save array as a separate file
            import numpy as np

            i = 1
            while True:
                dst = join(PATH_WORKSPACE, f'array#{i}.npy')

                if not root.has(dst):
                    break

                i += 1
            
            np.save(dst, self.data)

            return {'data': dst}
        
        return {'data': self.data}

    def __setstate__(self, state: dict):
        self.data = state['data']


define_matcher(test, Numpy)
