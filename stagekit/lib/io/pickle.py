import pickle


def load(src: str):
    with open(src, 'rb') as fb:
        return pickle.load(fb)


def dump(obj: object, dst: str):
    with open(dst, 'wb') as fb:
        pickle.dump(obj, fb)
