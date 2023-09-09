#!/usr/bin/env python
from sys import argv


def cli_run():
    from stagekit import ctx
    from stagekit.main import main
    from stagekit.config import config, PATH_PICKLE
    from stagekit.wrapper import StageFunc
    from importlib import import_module

    if '-r' in argv:
        ctx.rm(PATH_PICKLE)
    
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
    pass


def cli_log():
    pass


def cli_config():
    pass


def cli():
    if len(argv) > 1:
        if argv[1] == 'run' or argv[1] == 'r':
            cli_run()
