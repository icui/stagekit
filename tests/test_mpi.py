from time import sleep
from stagekit import stage, ctx, gather
from stagekit.mpi import stat
from sys import argv

@stage
async def test():
    # await test_mp()
    await test_mpi()


@stage
async def test_mp():
    fname = await ctx.mpiexec('echo serial_1 && sleep 2', multiprocessing=True)
    print(ctx.read(fname+'.stdout'))
    fname = await ctx.mpiexec(_sleep, args=('serial_2', 2), multiprocessing=True)
    print(ctx.read(fname+'.stdout'))
    fname = await ctx.mpiexec('echo parallel_1 && sleep 2', 2, multiprocessing=True)
    print(ctx.read(fname+'.stdout'))
    fname = await ctx.mpiexec(_sleep2, 2, args=('parallel_2', 2), mpiargs=('a1', 'a2'), multiprocessing=True)
    print(ctx.read(fname+'.stdout'))


@stage
async def test_mpi():
    fname = await ctx.mpiexec('echo serial_1 && sleep 2')
    print(ctx.read(fname+'.stdout'))
    fname = await ctx.mpiexec(_sleep, args=('serial_2', 2))
    print(ctx.read(fname+'.stdout'))
    fname = await gather(
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
    print(ctx.read(fname[0]+'.stdout'))
    print(ctx.read(fname[1]+'.stdout'))
    fname = await ctx.mpiexec(_sleep2, 2, args=('parallel_2', 2), mpiargs=('a1', 'a2'))
    print(ctx.read(fname+'.stdout'))


def _sleep(msg, dur):
    print(msg)
    sleep(dur)

def _sleep2(msg, dur):
    print(msg, stat.rank, stat.size, stat.mpiargs)
    sleep(dur)
