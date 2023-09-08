from .io import IO, define_io


class Pickle(IO):
    def __init__(self):
        from pickle import load, dump

        self._load = load
        self._dump = dump

    def load(self, src: str):
        with open(src, 'rb') as fb:
            return self._load(fb)


    def dump(self, obj: object, dst: str):
        with open(dst, 'wb') as fb:
            self._dump(obj, fb)


define_io(('pickle', 'pkl'), Pickle)
