#!/usr/bin/env python

import fabric.colors as fc
from fabric.decorators import task

import tasks
from tasks import *


__all__ = ['deploy', 'deploy_and_run', 'compilation', 'prepare', 'run', 'restart']
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
      check_code
    '''
    if environ['instrument']:
        check_code(environ)
        instrument_code(environ)
    elif check_code(environ):
        compile_model(environ)


@env_options
@task
def prepare(environ, **kwargs):
    '''Create all directories and put files in the right places.

    Depends on:
      prepare_expdir
      link_agcm_inputs
      prepare_workdir
    '''
    prepare_expdir(environ)
    if environ['type'] in ('coupled', 'atmos'):
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


@env_options
@task
def restart(environ, **kwargs):
    '''Restart the model.

    Depends on:
      restart_model
    '''
    restart_model(environ)
