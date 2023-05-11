from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stagekit.config import ConfigDict


default_config: ConfigDict = {
    'job': {
        'system': 'local',
        'name': 'stagekit',
        'nnodes': 1
    },
    'system': {
        'local': 'stagekit.lib.system.local'
    },
    'io': {
        'json': 'stagekit.lib.io.json',
        'toml': 'stagekit.lib.io.toml',
        'pickle': 'stagekit.lib.io.pickle',
        'pkl': 'stagekit.lib.io.pickle',
        'npy': 'stagekit.lib.io.numpy'
    },
    'data': {}
}
