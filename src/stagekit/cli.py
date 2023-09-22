#!/usr/bin/env python
from sys import argv, exit


def select(items: list, prompt: str = 'Select one of the following:'):
    """Choose from a list of items."""
    print(prompt)

    for i, item in enumerate(items):
        print(f'{i+1}) {item}')
    
    print(f'{len(items)+1}) Exit.')

    while True:
        try:
            i = input()

            if i == str(len(items) + 1):
                raise KeyboardInterrupt()

            idx = int(i) - 1
        
            assert idx >= 0 and idx < len(items)

        except KeyboardInterrupt:
            exit()
        
        except:
            print(f'Please input a number between 1 and {len(items)+1}')
        
        else:
            return i


def cli_run():
    """Create and execute a workflow from the stage function provided.
        Usage:
            `stagekit run (<function_module>:<function_name)`, e.g. `stagekit run tests.test_mpi:test`

        Flags:
            -r: Delete saved state and start a new workflow.
            -P: Do not add current folder to PYTHONPATH by default.
    """
    from importlib import import_module

    from .main import run
    from .directory import root, ws
    from .config import config, PATH_WORKSPACE
    from .wrapper import StageFunc
    from .stage import Stage

    if '-r' in argv:
        root.rm(PATH_WORKSPACE)
    
    if len(argv) > 2 and ':' in argv[2]:
        src = argv[2]
    
    elif 'main' in config:
        src = config['main']
    
    else:
        if not ws.has('stagekit.pickle'):
            try:
                src = input('Enter the main stage to run (<module_name>:<func_name>):\n')
            
            except KeyboardInterrupt:
                exit()
            
        else:
            src = None
    
    func = None

    if src:
        try:
            modname, funcname = src.replace('/', '.').split(':')
            func = getattr(import_module(modname), funcname)
            assert isinstance(func, StageFunc)
        
        except:
            print(f'Error: invalid function path: {src}.')
            print('Please check the path and make sure target function is wrapped by @stage.')
    
    stage = Stage(func, [], {}, None, 0) if func else None
    run(stage, True)


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
    from importlib import import_module
    from stagekit import ws
    from stagekit.config import config

    for src in config['modules']:
        import_module(src)

    print(repr(ws.load('stagekit.pickle')))


def cli_log_str(stage, indent):
    sp = '  ' * indent
    msg = ''

    if hasattr(stage.func.func, '__name__'):
        msg += stage.func.func.__name__ + '\n'
    
    else:
        msg += '<anonymous stage>' + '\n'

    children = []

    for s in stage.history:
        if s.parent_version == stage.version:
            children.append(s)
    
    nidx = 1 + len(str(len(children)))

    for i, s in enumerate(children):
        msg += sp + f'{i+1})' + ' ' * (nidx - len(str(i+1))) + cli_log_str(s, indent+1)
    
    return msg


def cli_config():
    """Change local or global configuration.
    """
    from .config import PATH_LOCAL, PATH_GLOBAL

    i = select([
        'Local configuration (for current workspace).',
        'Global configuration (for all workspaces of current user).'
    ], 'Choose whether you want to change local or global configuration:')

    if i == 1:
        prompt = 'current workspace'
        path = PATH_LOCAL
        env = 'STAGEKIT_CONFIG_LOCAL'

    else:
        prompt = 'all workspaces of current user'
        path = PATH_GLOBAL
        env = 'STAGEKIT_CONFIG_GLOBAL'

    print(f'Editing configuration for {prompt}, which is `{path}`.')
    print('If this is OK, press Enter to confirm.')
    print(f'If you want to change the location of the configuration file, input any character to cancel, then set environment variable `{env}`.')

    try:
        if input():
            return
    
    except KeyboardInterrupt:
        exit()

    match select([
        'Job configuration.',
        'Modules to load before execution.',
        'Main function of the workflow to execute.',
        'Data that can be accessed from `stagekit.ctx`.'
    ]):
        case 1:pass
        case 2: pass


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
