from .io import IO, define_io


class JSON(IO):
    def __init__(self):
        from json import load, dump

        self._load = load
        self._dump = dump

    def load(self, src: str):
        with open(src, 'r') as f:
            return self._load(f)


    def dump(self, obj: dict, dst: str):
        with open(dst, 'w') as f:
            self._dump(obj, f)


define_io('json', JSON)
