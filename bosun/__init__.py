#!/usr/bin/env python

import fabric.colors as fc
from fabric.decorators import task
from fabric.api import run as frun

from bosun import coupled, agcm, mom4
from bosun import tasks
from bosun.environ import env_options, fmt


@task
@env_options
def deploy(environ, **kwargs):
    '''Deploy cycle: prepare, compile.

    Depends on:
      prepare
      compilation
    '''
    print(fc.green("Started"))
    prepare(environ)
    compilation(environ)


@task
@env_options
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


@task
@env_options
def compilation(environ, **kwargs):
    '''Compile code for model run and post-processing.

    Depends on:
      instrument_code
      compile_model
      check_code
    '''
    if environ['instrument']:
        tasks.check_code(environ)
        tasks.instrument_code(environ)
    elif tasks.check_code(environ):
        tasks.compile_model(environ)


@task
@env_options
def prepare(environ, **kwargs):
    '''Create all directories and put files in the right places.

    Depends on:
      prepare_expdir
      link_agcm_inputs
      prepare_workdir
    '''
    print(fc.yellow('Preparing expdir'))
    frun(fmt('mkdir -p {expdir}', environ))
    frun(fmt('mkdir -p {execdir}', environ))

    environ['model'].prepare(environ)
    frun(fmt('rsync -rtL --progress {expfiles}/exp/{name}/* {expdir}', environ))


@task
@env_options
def run(environ, **kwargs):
    '''Run the model.

    Depends on:
      run_model
    '''
    tasks.run_model(environ)


@task
@env_options
def archive(environ, **kwargs):
    '''Archive the outputs.

    Depends on:
      archive_model
    '''
    tasks.archive_model(environ)


@task
@env_options
def restart(environ, **kwargs):
    '''Restart the model.

    Depends on:
      run_model
      prepare_restart
    '''
    environ['mode'] = 'warm'
    tasks.run_model(environ)


@task
@env_options
def generate_grid(environ, **kwargs):
    tasks.prepare_expdir(environ)
    tasks.check_code(environ)
    mom4.compile_pre(environ)
    mom4.generate_grid(environ)


@task
@env_options
def make_xgrids(environ, **kwargs):
    tasks.prepare_expdir(environ)
    tasks.check_code(environ)
    mom4.compile_pre(environ)
    mom4.make_xgrids(environ)


@task
@env_options
def regrid_3d(environ, **kwargs):
    tasks.prepare_expdir(environ)
    tasks.check_code(environ)
    mom4.compile_pre(environ)
    mom4.regrid_3d(environ)


@task
@env_options
def regrid_2d(environ, **kwargs):
    tasks.prepare_expdir(environ)
    tasks.check_code(environ)
    mom4.compile_pre(environ)
    mom4.regrid_2d(environ)
