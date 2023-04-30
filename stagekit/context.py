from .stage import Stage


class Context:
    """Getter of stage data and keyword arguments that also inherits from parent stages."""
    # stage currently being executed
    _current: Stage | None = None

    # root stage is being saved
    _saving = False

    def __getattr__(self, key):
        current = self._current

        while current:
            if key in current.data:
                return current.data[key]

            if key in current.config[2]:
                return current.config[2][key]

            current = current.parent

        if key in Stage.data:
            return Stage.data[key]

        return None

    def __setattr__(self, key, val):
        if key[0] == '_':
            self.__dict__[key] = val
        
        else:
            (self._current or Stage).data[key] = val
