from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Type, Dict, Callable, Any, Tuple

from stagekit.directory import root, ws
from stagekit.config import config


# dict of variable test function -> data class
_data_cls: Dict[Callable[..., bool], Type[Data]] = {}

# cache of data saved in separate files
_data_cache: Dict[int, list] = {0: []}

# size of data cache
_data_size: Dict[int, int] = {0: 0}

# whether the latest data chunk has been updated
_data_updated = False


class Data(ABC):
    """Base class of data wrapper for serialization and comparison."""
    # raw data object
    data: Any = None

    # index of the data in _data_cache
    location: Tuple[int, int] | None = None

    @property
    @abstractmethod
    def size(self) -> int:
        """Size of the raw data object."""

    def __init__(self, data):
        self.data = data
    
    def __getstate__(self):
        global _data_updated

        if self.location is None:
            size = self.size
            idx = len(_data_cache) - 1
            chunk = config['data_chunk_size']

            if _data_size[idx] > 0 and chunk and size + _data_size[idx] > chunk * 1024 ** 2:
                save_data()
                idx += 1
                _data_cache[idx] = []
                _data_size[idx] = 0

            self.location = idx, len(_data_cache[idx])

            _data_cache[idx].append(self.data)
            _data_size[idx] += size
            _data_updated = True

        return {'location': self.location}

    def load(self):
        if self.location and self.data is None:
            idx = self.location[0]
            if idx not in _data_cache or _data_size[idx] == 0:
                _data_cache[idx], _data_size[idx] = ws.load(f'data#{idx}.pickle')
            
            self.data = _data_cache[idx][self.location[1]]


def define_data(test: Callable[..., bool], obj: Type[Data]):
    """Define a data wrapper."""
    _data_cls[test] = obj


def save_data():
    """Save cached stage data to file."""
    global _data_updated

    idx = len(_data_cache) - 1

    if _data_updated:
        _data_updated = False
        print(f'data#{idx} saved: {_data_size[idx]/1024**2:.2f}MB')
        ws.dump((_data_cache[idx], _data_size[idx]), f'data#{idx}.pickle')
