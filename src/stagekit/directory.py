from __future__ import annotations

from os import path, fsync
from subprocess import check_call
from glob import glob
from typing import List, Collection, Awaitable, Any, Callable, Literal, Tuple, TYPE_CHECKING

from .io.io import get_io
from .config import PATH_WORKSPACE

if TYPE_CHECKING:
    from .mpiexec import MPIOutput


class Directory:
    """Utility for directory operation."""
    # working directory relative to root directory
    _cwd: str = '.'

    @property
    def cwd(self):
        """Current working directory."""
        return self._cwd

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

    def call(self, cmd: str, cwd: str | None = None) -> Awaitable[None]:
        """Call a shell command asynchronously.

        Args:
            cmd (str): Shell command.
            cwd (str | None): Directory to execute the command relative to current context. Defaults to None.
        """
        from .wrapper import call

        cwd = self.cwd if cwd is None else path.join(self.cwd, cwd)

        return call(cmd, cwd)

    def mpiexec(self, cmd: str | Callable,
            nprocs: int = 1, cpus_per_proc: int = 1, gpus_per_proc: int | Tuple[Literal[1], int] = 0, *,
            multiprocessing: bool = False, custom_exec: str | None = None, custom_nnodes: int | Tuple[int, int] | None = None,
            args: Collection | None = None, mpiargs: Collection | None = None, fname: str | None = None,
            check_output: Callable[..., None] | None = None, timeout: Literal['auto'] | float | None = 'auto',
            priority: int = 0) -> Awaitable[MPIOutput]:
        """Execute a function or shell command with MPI or multiprocessing.

        Args:
            cmd (str | Callable[[], Any] | Callable[[Any], Any]): Command or function to execute with MPI.
            nprocs (int, optional): Number of MPI processes. Defaults to 1.
            cpus_per_proc (int, optional): Number of CPUs per MPI processes. Defaults to 1.
            gpus_per_proc (int | Tuple[Literal[1], int], optional): Number of GPUs per MPI processes, use (1,n) for MPS (use one GPU for multiple MPI processes). Defaults to 0.
            multiprocessing (bool, optional): Use multiprocessing instead of MPI. Defaults to False.
            custom_exec (str | None, optional): Custom command to call MPI tasks. Defaults to None.
            custom_nnodes (int | Tuple[int, int] | None, optional): Specify the number of nodes if custom_exec is enabled. Defaults to None.
            args (Collection[Any] | None, optional): Arguments passed directly to task function. Defaults to None.
            mpiargs (Collection[Any] | None, optional): Arguments that can be accessed by task function through stagekit.subprocess.stat.mpiargs. Defaults to None.
            fname (str | None, optional): Name of the input/output files (e.g. {fname}.log, {fname}.pickle, {fname}.out). Defaults to None.
            check_output (Callable[..., None] | None): Check the output of stdout and/or stderr and determine if task succeeded.
            timeout (Literal['auto'] | float | None): Action when running out of walltime.
            priority (int | None, optional): Priority of the job execution. Defaults to None.
        """
        from .mpiexec import mpiexec

        return mpiexec(self.cwd, cmd,
            nprocs, cpus_per_proc, gpus_per_proc, multiprocessing,
            custom_exec, custom_nnodes, args, mpiargs, fname,
            check_output, timeout, priority)

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
    
    def writelines(self, lines: Collection[str], dst: str, mode: str = 'w', *, mkdir: bool = True):
        """Write lines of a text file.

        Args:
            lines (Collection[str]): Lines of the text file.
            dst (str): Relative path to the text file.
            mode (str, optional): Write mode. Defaults to 'w'.
            mkdir (bool, optional): Creates a new directory if dst does not exist. Defaults to True.
        """
        if mkdir:
            self.mkdir(path.dirname(dst))

        self.write('\n'.join(lines), dst, mode)

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
            ext = src.split('.')[-1]

        return get_io(ext).load(self.path(src))
    
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
        
        return get_io(ext).dump(obj, self.path(dst))


# reference to root directory
root = Directory()

# reference to workspace directory
ws = Directory()
ws._cwd = PATH_WORKSPACE
