from asyncio import sleep, gather
from stagekit import stage, ctx
from sys import argv


class Msg:
    def __init__(self, m):
        self.msg = m
    
    def __eq__(self, other):
        if isinstance(other, Msg):
            return self.msg == other.msg
        
        return False

    def __repr__(self):
        return self.msg


@stage(rerun=True)
async def inversion():
    print('inversion')
    await ctx.call(f'echo "start ({ctx.cwd})"')
    await gather(preproc_async1(Msg('download')), preproc_async2())
    mf = 0
    for i in range(5):
        ctx.setwd(f'iter_{i:02d}')
        mf += await iterate(iteration=i)
    ctx.setwd()
    await ctx.call(f'echo "end ({ctx.cwd})"')
    return mf


@stage(name=lambda d: f'iteration_{d["iteration"]+1}')
async def iterate(iteration: int):
    ctx.setwd('proc')
    await preprocess(iteration)
    ctx.setwd('specfem')
    await specfem(0)
    return iteration


@stage
async def preprocess(iteration):
    print(ctx.path(), iteration)


@stage
async def specfem(_):
    await sleep(1)
    print(ctx.cwd, ctx['iteration'])

    for i, a in enumerate(argv[:-1]):
        if a == '-k' and int(argv[i+1]) == ctx['iteration']:
            raise RuntimeError('expected error')


@stage
async def preproc_async1(msg):
    ctx['task'] = 'download'
    print(msg)
    await sleep(1)
    print(ctx.cwd, 'download', ctx['task'])


@stage
async def preproc_async2():
    ctx['task'] = 'meshfem'
    print('meshfem')
    await sleep(0.5)
    print(ctx.cwd, 'meshfem', ctx['task'])


if __name__ == '__main__':
    inversion()
