import json
from os import environ
from typing import Tuple, Dict, List, TypedDict

from .lib.config import config_default


class JobDict(TypedDict, total=False):
    """Job configuration."""
    # inherit from and existing configuration
    inherit: str

    # job name
    name: str

    # number of nodes to request and run MPI tasks (set to None if this is determined from nprocs)
    nnodes: int

    # total number of processes (set to None if this is determined from nnodes)
    nprocs: int

    # import path and function to run MPI command
    mpiexec: Tuple[str, str]

    # import path and function to write job script
    write: Tuple[str, str]

    # command to submit a job script
    submit: str


class FormatDict(TypedDict):
    """Custom format for ctx.load() and ctx.dump()."""
    # extension name
    ext: str | List[str]

    # import path and function name for reading the file
    load: Tuple[str, str]

    # import path and function name for saving the file
    dump: Tuple[str, str]


class ConfigDict(TypedDict):
    """Content of config.json."""
    # job configuration
    job: JobDict

    # custom format for ctx.load() and ctx.dump()
    io: Dict[str, FormatDict]

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


class Config:
    """Class for managing configurations."""
    # global configuration file
    json_global = environ.get('STAGEKIT_CONFIG_GLOBAL') or '~/.stagekit.config.json'

    # configuration file of current environment
    json_env = environ.get('STAGEKIT_CONFIG_ENV')

    # local configuration file
    json_local = environ.get('STAGEKIT_CONFIG_LOCAL') or 'stagekit.config.json'

    # plain dict loaded from json files
    raw_dict = config_default

    def __init__(self):
        # paths to load config from, priority: local > env > global
        paths = [self.json_global] +  (self.json_env.split(':') if self.json_env else []) + [self.json_local]

        for src in paths:
            try:
                with open(src, 'r') as f:
                    merge_dict(self.raw_dict, json.load(f))

            except:
                pass

    def save_local(self):
        pass

    def save_global(self):
        pass


config = Config()
