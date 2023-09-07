import tomllib
from os import environ
from typing import List, TypedDict


class Config(TypedDict):
    """Content of config.toml."""
    # job configuration (overwrites properties job object)
    job: dict

    # modules to import before execution
    modules: List[str]

    # default data that can be accessed by ctx[]
    data: dict


def merge_dict(a, b):
    for key in b:
        if isinstance(a.get(key), dict) and isinstance(b[key], dict):
            merge_dict(a[key], b[key])
        
        elif isinstance(a.get(key), list) and isinstance(b[key], list):
            a[key] += b[key]

        else:
            a[key] = b[key]


# global configuration file
path_global = environ.get('STAGEKIT_CONFIG_GLOBAL') or '~/.stagekit.config.toml'

# configuration file of current workspace
path_local = environ.get('STAGEKIT_CONFIG_LOCAL') or './config.toml'

# default config from stagekit module
config: Config = {
    'job': {
        'job': 'local'
    },
    'modules': [
        'stagekit.lib.job.local',
        'stagekit.lib.io.json',
        'stagekit.lib.io.toml',
        'stagekit.lib.io.pickle',
        'stagekit.lib.io.numpy'
    ],
    'data': {}
}

# paths to load config from, priority: local > env > global
for src in path_global, path_local:
    try:
        with open(src, 'rb') as f:
            merge_dict(config, tomllib.load(f))

    except:
        pass
