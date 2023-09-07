from tests.test_mpi import test
from stagekit import ctx
from sys import argv

if __name__ == '__main__':
    if len(argv) == 1:
        ctx.rm('stagekit.pickle')

    test()
