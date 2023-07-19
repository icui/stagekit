import tomllib
from os import environ
from typing import Dict, TypedDict, NotRequired

from .lib.config import default_config


class JobDict(TypedDict):
    """Job configuration"""
    # Job scheduler
    system: str

    # job name
    name: NotRequired[str]

    # number of nodes to request and run MPI tasks, defaults to 1
    nnodes: NotRequired[int]

    # overwrite cpus_per_node defined by system
    cpus_per_node: NotRequired[int]

    # overwrite gpus_per_node defined by system
    gpus_per_node: NotRequired[int]


class ConfigDict(TypedDict):
    """Content of config.toml."""
    # job configuration
    job: JobDict

    # job scheduler configuration
    system: Dict[str, str]

    # custom format for ctx.load() and ctx.dump()
    io: Dict[str, str]

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

# configuration file of current environment
path_env = environ.get('STAGEKIT_CONFIG_ENV')

# configuration file of current workspace
path_local = environ.get('STAGEKIT_CONFIG_LOCAL') or 'stagekit.config.toml'

# default config from stagekit module
config = default_config

# paths to load config from, priority: local > env > global
paths = [path_global] +  (path_env.split(':') if path_env else []) + [path_local]


for src in paths:
    try:
        with open(src, 'rb') as f:
            merge_dict(config, tomllib.load(f))

    except:
        pass
