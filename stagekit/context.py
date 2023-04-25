from typing import Any

from .stage import Stage


class Context:
    """Getter of stage data and keyword arguments that also inherits from parent stages."""
    def __getattr__(self, key: str):
        current = Stage.current

        while current:
            if key in current.data:
                return current.data[key]

            if key in current.config[2]:
                return current.config[2][key]

            current = current.parent

        if key in Stage.data:
            return Stage.data[key]

        return None

    def __setattr__(self, key: str, val: Any):
        if Stage.current:
            Stage.current.data[key] = val

        else:
            Stage.data[key] = val
