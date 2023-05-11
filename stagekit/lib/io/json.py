import json


def load(src: str):
    with open(src, 'r') as f:
        return json.load(f)


def dump(obj: dict, dst: str):
    with open(dst, 'w') as f:
        json.dump(obj, f)
