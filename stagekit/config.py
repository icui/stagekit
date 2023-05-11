import tomllib
from os import environ
from typing import Tuple, Dict, List, TypedDict, NotRequired

from .lib.config import default_config


class ConfigDictJob(TypedDict):
    """Job configuration"""
    # Job scheduler
    system: str

    # job name
    name: NotRequired[str]

    # number of nodes to request and run MPI tasks (set to None if this is determined from nprocs)
    nnodes: int


# class ConfigDictSystem(TypedDict, total=False):
#     """Job scheduler configuration."""
#     # inherit from and existing configuration
#     system: str

#     # whether a node can be shared by multiple MPI calls
#     share_node: bool

#     # whether a gpu can be shared by multiple MPI processes (multi-process service)
#     share_gpu: bool

#     # number of cpus per node
#     cpus_per_node: int

#     # number of gpus per node
#     gpus_per_node: int

#     # module that contains mpiexec() and write()
#     module: str

#     # command to submit a job script
#     submit: str



class ConfigDict(TypedDict):
    """Content of config.toml."""
    # job configuration
    job: ConfigDictJob

    # job scheduler
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
