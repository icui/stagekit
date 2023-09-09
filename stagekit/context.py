from __future__ import annotations

from os import path
from asyncio import sleep
from traceback import format_exc
from sys import stderr

from .stage import Stage, current_stage
from .directory import Directory
from .subprocess import stat
from .config import PATH_PICKLE


class Context(Directory):
    """Getter of stage keyword arguments that also inherits from parent stages."""
    # reference to root directory
    root: Directory

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

    def __init__(self):
        self.root = Directory()

    def __getitem__(self, key):
        current = current_stage()

        while current:
            if key in current.data:
                return current.data[key]

            if key in current.kwargs:
                return current.kwargs[key]

            current = current.parent

        return Stage.data.get(key)

    def __setitem__(self, key, val):
        (current_stage() or Stage).data[key] = val

    def setwd(self, cwd: str | None = None):
        """Set working directory.

        Args:
            cwd (str | None): Directory path relative to the base directory of current stage.
        """
        if not hasattr(self, 'root'):
            raise RuntimeError('cannot change root directory')

        self._chdir = cwd

    @property
    def rerun(self):
        """Stage has previously been executed, re-running because stage has rerun flag on."""
        if stage := current_stage():
            return stage.rerun
        
        return False

    async def checkpoint(self):
        """Save root stage to stagekit.pickle one second later."""
        if self._saving:
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
        if not stat.in_subprocess:
            self.root.dump(stage, '_stagekit.pickle')

            try:
                # verify saved state
                s = self.root.load('_stagekit.pickle')
                assert s == stage

            except Exception:
                print(format_exc(), file=stderr)

            else:
                self.root.mv('_stagekit.pickle', PATH_PICKLE)

            self._saving = False
