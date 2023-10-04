#!/bin/sh
stagekit run tests.test_mpi:test
stagekit run tests.test:inversion
python tests/sp.py