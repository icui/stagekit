from __future__ import annotations

from os import path, fsync
from subprocess import check_call
from glob import glob
import pickle
import toml
from typing import List, Iterable, Any

from .stage import Stage


class Context:
    """Getter of stage keyword arguments that also inherits from parent stages."""
    # stage currently being executed
    _current: Stage | None = None

    # root stage is being saved
    _saving = False

    # working directory relative to current stage directory
    _chdir: str | None = None

    # working directory relative to root directory
    _cwd: str = '.'

    def __getitem__(self, key):
        current = self._current

        while current:
            if key in current.data:
                return current.data[key]

            if key in current.config[2]:
                return current.config[2][key]

            current = current.parent

        return Stage.data.get(key)

    def __setitem__(self, key, val):
        (self._current or Stage).data[key] = val

    @property
    def cwd(self):
        """Current working directory."""
        return self._cwd

    def goto(self, cwd: str | None = None):
        """Change working directory.

        Args:
            cwd (str | None): Directory path relative to the base directory of current stage.
        """
        self._chdir = cwd

        paths = []

        if self._chdir is not None:
            paths.append(self._chdir)

        current = self._current

        while current:
            if current.config[3] is not None:
                paths.append(current.config[3])
            
            current = current.parent
        
        paths.append('.')
        paths.reverse()

        self._cwd = path.normpath(path.join(*paths))
    
    def path(self, *paths: str) -> str:
        """Get relative path of target directory.

        Args:
            *paths (str): Paths to child directory.

        Returns:
            str: Path of target directory.
        """
        return path.normpath(path.join(self.cwd, *paths))
    
    def abspath(self, *paths: str) -> str:
        """Get absolute path of target directory.

        Args:
            *paths (str): Paths to child directory.
            abs (bool, optional): Return absolute path instead. Defaults to False.

        Returns:
            str: Path of target directory.
        """
        return path.abspath(self.path(*paths))
    
    def relpath(self, *paths: str) -> str:
        """Convert from a path relative to root directory to a path relative to current directory.

        Args:
            *paths (str): Path relative to root directory.

        Returns:
            str: Relative path from self to input path.
        """
        src = path.join(*paths)

        if path.isabs(src):
            return src

        return path.relpath(src or '.', self.cwd)

    def has(self, src: str = '.') -> bool:
        """Check if a file or a directory exists.

        Args:
            src (str, optional): Relative path to the file or directory. Defaults to '.'.

        Returns:
            bool: Whether file exists.
        """
        return path.exists(self.path(src))
    
    def rm(self, src: str = '.'):
        """Remove a file or a directory.

        Args:
            src (str, optional): Relative path to the file or directory. Defaults to '.'.
        """
        check_call('rm -rf ' + self.path(src), shell=True)
    
    def cp(self, src: str, dst: str = '.', *, mkdir: bool = True):
        """Copy file or a directory.

        Args:
            src (str): Relative path to the file or directory to be copied.
            dst (str, optional): Relative path to the destination directory. Defaults to '.'.
            mkdir (bool, optional): Whether or not create a new directory if dst does not exist. Defaults to True.
        """
        if mkdir:
            self.mkdir(path.dirname(dst))

        check_call(f'cp -r {self.path(src)} {self.path(dst)}', shell=True)
    
    def mv(self, src: str, dst: str = '.', *, mkdir: bool = True):
        """Move a file or a directory.

        Args:
            src (str): Relative path to the file or directory to be moved.
            dst (str, optional): Relative path to the destination directory. Defaults to '.'.
            mkdir (bool, optional): Whether or not create a new directory if dst does not exist. Defaults to True.
        """
        if mkdir:
            self.mkdir(path.dirname(dst))

        check_call(f'mv {self.path(src)} {self.path(dst)}', shell=True)
    
    def ln(self, src: str, dst: str = '.', mkdir: bool = True):
        """Link a file or a directory.

        Args:
            src (str): Relative path to the file or directory to be linked.
            dst (str, optional): Relative path to the destination directory. Defaults to '.'.
            mkdir (bool, optional): Whether or not create a new directory if dst does not exist. Defaults to True.
        """
        # source file name
        srcdir = path.dirname(src) or '.'
        srcf = path.basename(src)

        # determine target directory and file name
        if self.isdir(dst):
            self.rm(path.join(dst, srcf))
            dstdir = dst
            dstf = '.'
        
        else:
            self.rm(dst)
            dstdir = path.dirname(dst) or '.'
            dstf = path.basename(dst)

            if mkdir:
                self.mkdir(dstdir)

        # relative path from source directory to target directory
        if not path.isabs(src):
            if not path.isabs(dst):
                # convert to relative path if both src and dst are relative
                src = path.join(path.relpath(srcdir, dstdir), srcf)
            
            else:
                # convert src to abspath if dst is abspath
                src = self.abspath(src)

        check_call(f'ln -s {src} {dstf}', shell=True, cwd=self.path(dstdir))
    
    def mkdir(self, dst: str = '.'):
        """Create a new directory recursively.

        Args:
            dst (str, optional): Relative path to the directory to be created. Defaults to '.'.
        """
        check_call('mkdir -p ' + self.path(dst), shell=True)
    
    def ls(self, src: str = '.', grep: str = '*', isdir: bool | None = None) -> List[str]:
        """List items in a directory.

        Args:
            src (str, optional): Relative path to target directed. Defaults to '.'.
            grep (str, optional): Patten to filter listed items. Defaults to '*'.
            isdir (bool | None, optional): True: list directories only, False: list files only,
                None: list both files and directories. Defaults to None.

        Returns:
            List[str]: Items in the directory.
        """
        entries: List[str] = []

        for entry in glob(self.path(path.join(src, grep))):
            # skip non-directory entries
            if isdir is True and not path.isdir(entry):
                continue
            
            # skip directory entries
            if isdir is False and path.isdir(entry):
                continue
            
            entries.append(entry.split('/')[-1])

        return entries
    
    def isdir(self, src: str = '.') -> bool:
        """Check if src is a directory.

        Args:
            src (str, optional): Relative path to be checked. Defaults to '.'.

        Returns:
            bool: Whether src is a directory.
        """
        return path.isdir(self.path(src))

    def read(self, src: str) -> str:
        """Read text file.

        Args:
            src (str): Path to the text file.

        Returns:
            str: Content to the text file.
        """
        with open(self.path(src), 'r', errors='ignore') as f:
            return f.read()

    def write(self, text: str, dst: str, mode: str = 'w', *, mkdir: bool = True):
        """Write a text file.

        Args:
            text (str): Content of the text file.
            dst (str): Relative path to the text file.
            mode (str, optional): Write mode. Defaults to 'w'.
            mkdir (bool, optional): Creates a new directory if dst does not exist. Defaults to True.
        """
        if mkdir:
            self.mkdir(path.dirname(dst))

        with open(self.path(dst), mode) as f:
            f.write(text)
            f.flush()
            fsync(f.fileno())
    
    def readlines(self, src: str) -> List[str]:
        """Read lines of a text file.

        Args:
            src (str): Relative path to the text file.

        Returns:
            List[str]: Lines of the text file.
        """
        return self.read(src).split('\n')
    
    def writelines(self, lines: Iterable[str], dst: str, mode: str = 'w', *, mkdir: bool = True):
        """Write lines of a text file.

        Args:
            lines (Iterable[str]): Lines of the text file.
            dst (str): Relative path to the text file.
            mode (str, optional): Write mode. Defaults to 'w'.
            mkdir (bool, optional): Creates a new directory if dst does not exist. Defaults to True.
        """
        if mkdir:
            self.mkdir(path.dirname(dst))

        self.write('\n'.join(lines), dst, mode)
    
    def call(self, cmd: str):
        """Call a shell command.

        Args:
            cmd (str): Shell command.
        """
        check_call(cmd, cwd=self.cwd, shell=True)
    
    async def async_call(self, cmd: str):
        """Call a shell command asynchronously.

        Args:
            cmd (str): Shell command.
        """
        from asyncio import create_subprocess_shell
        process = await create_subprocess_shell(cmd, cwd=self.cwd)
        await process.communicate()
    
    def load(self, src: str, ext: str | None = None) -> Any:
        """Load a pickle / toml / json / npy file.

        Args:
            src (str): Relative path to the file.
            ext (str, optional): Type of the file to be read, use None to determine from file name. Defaults to None.

        Raises:
            TypeError: Unsupporte file type.

        Returns:
            Any: Content of the file.
        """
        if ext is None:
            ext = src.split('.')[-1] # type: ignore
        
        if ext == 'pickle':
            with open(self.path(src), 'rb') as fb:
                return pickle.load(fb)
        
        elif ext == 'toml':
            with open(self.path(src), 'r') as f:
                return toml.load(f)
        
        elif ext == 'json':
            import json
            with open(self.path(src), 'r') as f:
                return json.load(f)
        
        elif ext == 'npy':
            import numpy as np
            return np.load(self.path(src))
        
        else:
            raise TypeError(f'unsupported file type {ext}')
    
    def dump(self, obj, dst: str, ext: str | None = None, *, mkdir: bool = True):
        """Dump a pickle / toml / json / npy file.

        Args:
            obj (Any): Object to be dumped.
            dst (str): Relative path to the file.
            ext (str, optional): Type of the file to be dumped, use None to determine from file name. Defaults to None.

        Raises:
            TypeError: Unsupporte file type.
        """
        if mkdir:
            self.mkdir(path.dirname(dst))

        if ext is None:
            ext = dst.split('.')[-1]

        if ext == 'pickle':
            with open(self.path(dst), 'wb') as fb:
                pickle.dump(obj, fb)
        
        elif ext == 'toml':
            with open(self.path(dst), 'w') as f:
                toml.dump(obj, f)
        
        elif ext == 'json':
            import json
            with open(self.path(dst), 'w') as f:
                json.dump(obj, f)
        
        elif ext == 'npy':
            import numpy as np
            return np.save(self.path(dst), obj)
        
        else:
            raise TypeError(f'unsupported file type {ext}')
