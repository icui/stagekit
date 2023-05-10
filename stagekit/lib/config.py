from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stagekit.config import ConfigDict


default_config: ConfigDict = {
    'mpi': {
        'name': 'stagekit',
        'nprocs': 1,
        'mpiexec': ('stagekit.lib.clusters', 'local_mpiexec'),
        'write': ('stagekit.lib.clusters', 'local_write'),
        'submit': 'python'
    },
    'io': {
        'json': {
            'ext': 'json',
            'load': ('stagekit.lib.io', 'json_load'),
            'dump': ('stagekit.lib.io', 'json_dump')
        },
        'toml': {
            'ext': 'toml',
            'load': ('stagekit.lib.io', 'toml_load')
        },
        'pickle': {
            'ext': ['pickle', 'pkl'],
            'load': ('stagekit.lib.io', 'pickle_load'),
            'dump': ('stagekit.lib.io', 'pickle_dump')
        },
        'numpy': {
            'ext': 'npy',
            'load': ('stagekit.lib.io', 'numpy_load'),
            'dump': ('stagekit.lib.io', 'numpy_dump')
        }
    },
    'data': {}
}
