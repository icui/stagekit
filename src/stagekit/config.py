import tomllib
from os import environ
from typing import List, Literal, TypedDict, NotRequired
from os.path import expanduser


class Config(TypedDict):
    """Content of config.toml."""
    # modules to import before execution
    modules: List[str]

    # import path to the main function (<module_name>:<func_name>)
    main: NotRequired[str]

    # default re-run behavior 
    rerun_strategy: bool | Literal['auto']

    # save cached data to a separate file when the buffer size is larger than a specific value (in MB)
    data_chunk_size: int | float | None

    # job configuration (overwrites properties job object)
    job: dict

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
PATH_GLOBAL = expanduser(environ.get('STAGEKIT_CONFIG_GLOBAL') or '~/.stagekit.config.toml')

# configuration file of current workspace
PATH_LOCAL = environ.get('STAGEKIT_CONFIG_LOCAL') or 'config.toml'

# object containing execution status
PATH_WORKSPACE = environ.get('STAGEKIT_CONFIG_WORKSPACE') or '.stagekit'

# default config from stagekit module
config: Config = {
    'rerun_strategy': False,
    'data_chunk_size': None,
    'modules': [
        'stagekit.job.local',
        'stagekit.job.slurm',
        'stagekit.io.json',
        'stagekit.io.toml',
        'stagekit.io.pickle',
        'stagekit.io.numpy',
        'stagekit.data.numpy',
        'stagekit.data.function'
    ],
    'job': {
        'job': 'local'
    },
    'data': {}
}

# paths to load config from, priority: local > env > global
for src in PATH_GLOBAL, PATH_LOCAL:
    try:
        with open(src, 'rb') as f:
            merge_dict(config, tomllib.load(f))

    except:
        pass
