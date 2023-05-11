import tomllib

def load(src: str):
    with open(src, 'rb') as fb:
        return tomllib.load(fb)
