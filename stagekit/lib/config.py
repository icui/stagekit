from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stagekit.config import ConfigDict


default_config: ConfigDict = {
    'mpi': {
        'name': 'stagekit',
        'nprocs': 1,
        'mpiexec': ('stagekit.lib.clusters.local', 'mpiexec'),
        'write': ('stagekit.lib.clusters.local', 'write'),
        'submit': 'python'
    },
    'io': {
        'json': {
            'ext': 'json',
            'load': ('stagekit.lib.io.json', 'load'),
            'dump': ('stagekit.lib.io.json', 'dump')
        },
        'toml': {
            'ext': 'toml',
            'load': ('stagekit.lib.io.toml', 'load')
        },
        'pickle': {
            'ext': ['pickle', 'pkl'],
            'load': ('stagekit.lib.io.pickle', 'load'),
            'dump': ('stagekit.lib.io.pickle', 'dump')
        },
        'numpy': {
            'ext': 'npy',
            'load': ('stagekit.lib.io.numpy', 'load'),
            'dump': ('stagekit.lib.io.numpy', 'dump')
        }
    },
    'data': {}
}
