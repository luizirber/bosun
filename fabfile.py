#!/usr/bin/env python

import fabric.colors as fc
from fabric.decorators import task

import tasks
from tasks import *


__all__ = ['deploy', 'deploy_and_run', 'compilation', 'prepare', 'run']
__all__ += tasks.__all__
__all__.remove('env_options')


@env_options
@task
def deploy(environ, **kwargs):
    '''Deploy cycle: prepare, compile.

    Depends on:
      prepare
      compilation
    '''
    print(fc.green("Started"))
    prepare(environ)
    compilation(environ)


@env_options
@task
def deploy_and_run(environ, **kwargs):
    '''Full model cycle: prepare, compile and run.

    Depends on:
      prepare
      compilation
      run
    '''
    print(fc.green("Started"))
    prepare(environ)
    compilation(environ)
    run(environ)


@env_options
@task
def compilation(environ, **kwargs):
    '''Compile code for model run and post-processing.

    Depends on:
      instrument_code
      compile_model
    '''
    if environ['instrument']:
        instrument_code(environ)
    else:
        compile_model(environ)


@env_options
@task
def prepare(environ, **kwargs):
    '''Create all directories and put files in the right places.

    Depends on:
      prepare_expdir
      check_code
      link_agcm_inputs
      prepare_workdir
    '''
    prepare_expdir(environ)
    check_code(environ)
    link_agcm_inputs(environ)
    prepare_workdir(environ)


@env_options
@task
def run(environ, **kwargs):
    '''Run the model.

    Depends on:
      run_model
    '''
    run_model(environ)
