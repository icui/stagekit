from __future__ import annotations
from typing import List, TYPE_CHECKING
from os.path import join
from .config import PATH_WORKSPACE
from .directory import root


if TYPE_CHECKING:
    from .stage import Stage


# cache of stagekit.pickle
_cache: List[Stage] | None = None


def load_cache() -> List[Stage]:
    global _cache

    if _cache is None:
        path_pkl = join(PATH_WORKSPACE, 'stagekit.pickle')

        if root.has(path_pkl):
            _cache = root.load(path_pkl)
        
        else:
            _cache = []
    
    return _cache # type: ignore
