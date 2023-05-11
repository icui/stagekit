import numpy as np


def load(src: str):
    return np.load(src)


def dump(obj: np.ndarray, dst: str):
    np.save(dst, obj)
