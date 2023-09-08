from .io import IO, define_io


class Toml(IO):
    def __init__(self):
        from tomllib import load

        self._load = load

    def load(self, src: str):
        with open(src, 'rb') as fb:
            return self._load(fb)


define_io('toml', Toml)
