from stagekit import ctx, stage
import asyncio
import numpy as np
from scipy.optimize import minimize
from time import sleep
from random import randint


@stage()
async def sleep_stage():
    pass
    # sleep(randint(1, 10))


@stage
async def rosen(x):
    """The Rosenbrock function"""
    print('>', x)

    await sleep_stage()

    return sum(100.0*(x[1:]-x[:-1]**2.0)**2.0 + (1-x[:-1])**2.0)


def minimization(i, x0):
    print(i, x0)
    minimize(rosen, x0, method='BFGS',
               options={'gtol': 1, 'disp': True})


def main():
    x0 = np.array([1.3, 0.7, 0.8, 1.9, 1.2])

    for i in range(1):
        minimization(i, x0)


if __name__ == '__main__':
    main()
