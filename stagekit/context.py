from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .stage import Stage


class Context:
    """Getter of keyword arguments that also inherits from parent stages."""
    _current: Stage | None = None
    
    def __getattr__(self, key: str):
        current = self._current
        
        while current:
            if key in current.config[2]:
                return current.config[2][key]
            
            current = current.parent
        
        return None


ctx = Context()

__all__ = ['ctx']
