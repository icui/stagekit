from __future__ import annotations
from typing import List, TYPE_CHECKING
from os.path import join
from .directory import ws


if TYPE_CHECKING:
    from .stage import Stage


# cache of stagekit.pickle
_cache: List[Stage] | None = None


def load_cache() -> List[Stage]:
    global _cache

    if _cache is None:
        if ws.has('stagekit.pickle'):
            _cache = ws.load('stagekit.pickle')
        
        else:
            _cache = []
    
    return _cache # type: ignore
