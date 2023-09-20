from __future__ import annotations

from os import path
from asyncio import sleep
from traceback import format_exc
from sys import stderr

from .stage import Stage, current_stage
from .directory import Directory, root
from .mpistat import stat
from .config import config, PATH_WORKSPACE
from .cache import load_cache


class Context(Directory):
    """Getter of stage keyword arguments that also inherits from parent stages."""
    # root stage is being saved
    # 0: not being saved
    # 1: preparing to save
    # 2: being saved
    _saving = False

    # working directory relative to current stage directory
    _chdir: str | None = None

    @property
    def cwd(self):
        """Current working directory."""
        paths = []

        if self._chdir is not None:
            paths.append(self._chdir)

        current = current_stage()

        while current:
            if current.cwd is not None:
                paths.append(current.cwd)
            
            current = current.parent
        
        paths.append('.')
        paths.reverse()

        return path.normpath(path.join(*paths))

    def __getitem__(self, key):
        current = current_stage()

        while current:
            if key in current.data:
                return current.data[key]

            if key in current.kwargs:
                return current.kwargs[key]

            current = current.parent

        return config['data'].get(key)

    def __setitem__(self, key, val):
        current = current_stage()

        if not current:
            raise RuntimeError('cannot set properties outside a running stage')

        current.data[key] = val

    def setwd(self, cwd: str | None = None):
        """Set working directory.

        Args:
            cwd (str | None): Directory path relative to the base directory of current stage.
        """
        self._chdir = cwd

    async def checkpoint(self):
        """Save root stage to stagekit.pickle one second later."""
        if self._saving or stat.in_subprocess:
            return

        if stage := current_stage():
            self._saving = True

            while stage.parent:
                stage = stage.parent

            await sleep(1)

            if self._saving:
                self._save(stage)

    def _save(self, stage: Stage):
        """Save a stage to stagekit.pickle."""
        if stat.in_subprocess:
            return

        path_tmp = path.join(PATH_WORKSPACE, '_stagekit.pickle')
        path_pkl = path.join(PATH_WORKSPACE, 'stagekit.pickle')

        stages = load_cache()
        replaced = False

        for i, s in enumerate(stages):
            if s == stage:
                stages[i] = s
                replaced = True
                break
        
        if not replaced:
            stages.insert(0, stage)

        root.dump(stages, path_tmp)

        try:
            # verify saved state
            assert root.load(path_tmp) == stages

        except Exception:
            print(format_exc(), file=stderr)

        else:
            root.mv(path_tmp, path_pkl)

        self._saving = False
