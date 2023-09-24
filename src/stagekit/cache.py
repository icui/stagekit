from __future__ import annotations
from typing import List, TYPE_CHECKING
from sys import path
from importlib import import_module

from .directory import ws
from .config import config


if TYPE_CHECKING:
    from .stage import Stage


# cache of stagekit.pickle
_cache: List[Stage] | None = None


def load_cache() -> List[Stage]:
    for src in config['modules']:
        import_module(src)

    global _cache

    if _cache is None:
        if ws.has('stagekit.pickle'):
            _cache = ws.load('stagekit.pickle')
        
        else:
            _cache = []
    
    return _cache # type: ignore
