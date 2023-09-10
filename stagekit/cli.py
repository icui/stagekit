#!/usr/bin/env python
from sys import argv


def cli_run():
    """Create and execute a workflow from the stage function provided.
        Usage:
            `stagekit run (<function_module>:<function_name)`, e.g. `stagekit run tests.test_mpi:test`

        Flags:
            -r: Delete saved state and start a new workflow.
    """
    from importlib import import_module

    from .main import main
    from .config import config, PATH_WORKSPACE
    from .wrapper import ctx, StageFunc

    if '-r' in argv:
        ctx.rm(PATH_WORKSPACE)
    
    if len(argv) > 2 and ':' in argv[2]:
        src = argv[2]
    
    elif 'main' in config:
        src = config['main']
    
    else:
        src = input('Enter the main stage to run (<module_name>:<func_name>):\n')
    
    func = None

    try:
        modname, funcname = src.replace('/', '.').split(':')
        func = getattr(import_module(modname), funcname)
        assert isinstance(func, StageFunc)
    
    except:
        print(f'Error: invalid function path: {src}.')
        print('Please check the path and make sure target function is wrapped by @stage.')
    
    if func:
        main(func)


def cli_help():
    """List available commands.
    """
    for cmd, func in commands.items():
        print(cmd)
        print('    ' + func.__doc__)
    
    print()


def cli_log():
    """Show the execution status of the workflow in current directory.
        Flags:
            -a: Expand all entries.
    """


def cli_config():
    """Change local or global configuration.
    """
    from .config import PATH_LOCAL, PATH_GLOBAL

    print('Choose whether you want to change local or global configuration:')
    print('1) Local configuration (for current workspace).')
    print('2) Global configuration (for all workspaces of current user).')
    print('3) Exit.')

    while True:
        mode = input()

        if mode == '1':
            cli_config_write('current workspace', PATH_LOCAL, 'STAGEKIT_CONFIG_LOCAL')
            break

        elif mode == '2':
            cli_config_write('all workspaces of current user', PATH_GLOBAL, 'STAGEKIT_CONFIG_GLOBAL')
            break

        elif mode == '3':
            break

        else:
            print('Please input `1`, `2`, or `3`.')


def cli_config_write(prompt: str, path: str, env: str):
    print(f'Editing configuration for {prompt}, which is `{path}`.')
    print('If this is OK, press Enter to confirm.')
    print('If you want to change the location of the configuration file, input any character to cancel,')
    print(f'  then set environment variable `{env}`.')

    if input():
        return

    print('Select item to config:')
    print('1) Job configuration.')
    print('2) Modules to load before execution.')
    print('3) Main function of the workflow to execute.')
    print('4) Default data in `stagekit.ctx`.')
    print('5) Exit.')

    while True:
        mode = input()

        if mode == '1':
            break

        elif mode == '2':
            break

        elif mode == '3':
            break

        else:
            print('Please input `1`, `2`, `3`, `4` or `5`.')


def cli_write():
    """Write execution command as a job script."""


commands = {
    'run': cli_run,
    'help': cli_help,
    'log': cli_log,
    'config': cli_config,
    'write': cli_write
}


def cli():
    if len(argv) > 1:
        cmd = argv[1]

    else:
        cmd = 'help'
    
    if cmd in commands:
        commands[cmd]()
        return
    
    elif len(cmd) == 1:
        for key in commands:
            if key[0] == cmd:
                commands[key]()
                return
    
    cli_help()
