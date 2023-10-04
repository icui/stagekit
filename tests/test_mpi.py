from time import sleep
from stagekit import stage, ctx, ws, gather
from stagekit.subprocess.stat import stat
import numpy as np

@stage
async def test():
    # await test_mp()
    await test_mpi()


@stage
async def test_mp():
    o = await ctx.mpiexec('echo serial_1 && sleep 2', multiprocessing=True)
    print(o.stdout)
    o = await ctx.mpiexec(_sleep, args=('serial_2', 2), multiprocessing=True)
    print(o.stdout)
    o = await ctx.mpiexec(_sleep, args=(np.array('serial_3'), 2), multiprocessing=True)
    print(o.stdout)
    o = await ctx.mpiexec('echo parallel_1 && sleep 2', 2, multiprocessing=True)
    print(o.stdout)
    o = await ctx.mpiexec(_sleep2, 2, args=('parallel_2', 2), mpiargs=('a1', 'a2'), multiprocessing=True)
    print(o.stdout)


@stage
async def test_mpi():
    o = await ctx.mpiexec('echo serial_1 && sleep 2')
    print(o.stdout)
    o = await ctx.mpiexec(_sleep, args=('serial_2', 2))
    print(o.stdout)
    o = await ctx.mpiexec(_sleep, args=(np.array('serial_3'), 2))
    print(o.stdout)
    o = await gather(
        ctx.mpiexec('echo parallel_1_1 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_2 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_3 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_4 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_5 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_6 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_7 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_8 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_9 && sleep 2', 2),
        ctx.mpiexec('echo parallel_1_10 && sleep 2', 2))
    print(o[0].stdout)
    print(o[1].stdout)
    o = await ctx.mpiexec(_sleep2, 2, args=('parallel_2', 2), mpiargs=('a1', 'a2'))
    print(o.stdout)


def _sleep(msg, dur):
    print(msg)
    sleep(dur)

def _sleep2(msg, dur):
    print(msg, stat.rank, stat.size, stat.mpiargs)
    sleep(dur)


if __name__ == '__main__':
    test()
