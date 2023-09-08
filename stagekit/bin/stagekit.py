#!/usr/bin/env python
from sys import argv


def cli_run():
    from stagekit import ctx
    from stagekit.main import main
    from stagekit.config import config
    from importlib import import_module

    if '-r' in argv:
        ctx.rm('stagekit.pickle')

    mod, func = config['main'].split(':')
    main(getattr(import_module(mod), func))


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
