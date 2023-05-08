from __future__ import annotations

from os import path
from asyncio import sleep
from traceback import format_exc
from sys import stderr

from .stage import Stage, current_stage
from .directory import Directory
from .execute import STAGE_IN_SUBPROCESS


class Context(Directory):
    """Getter of stage keyword arguments that also inherits from parent stages."""
    # reference to root directory
    root: Directory

    # root stage is being saved
    _saving = False

    # working directory relative to current stage directory
    _chdir: str | None = None

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

    def __eq__(self, ctx):
        return self._chdir == ctx._chdir

    def goto(self, cwd: str | None = None):
        """Change working directory.

        Args:
            cwd (str | None): Directory path relative to the base directory of current stage.
        """
        if not hasattr(self, 'root'):
            raise RuntimeError('cannot change root directory')

        self._chdir = cwd

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

        self._cwd = path.normpath(path.join(*paths))

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
                await self._save(stage)

    async def _save(self, stage: Stage):
        """Save a stage to stagekit.pickle."""
        if not STAGE_IN_SUBPROCESS:
            self.root.dump(stage, '_stagekit.pickle')
            await sleep(1)

            try:
                # verify saved state
                s = self.root.load('_stagekit.pickle')
                assert s == stage

            except Exception:
                print(format_exc(), file=stderr)

            else:
                self.root.mv('_stagekit.pickle', 'stagekit.pickle')

            self._saving = False
