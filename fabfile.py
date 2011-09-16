#!/usr/bin/env python

import fabric.colors as fc
from fabric.decorators import task

from tasks import *


@env_options
@task
def deploy(environ, **kwargs):
    print(fc.green("Started"))
    compilation(environ)
    prepare(environ)
    run(environ)


@env_options
@task
def compilation(environ, **kwargs):
    prepare_expdir(environ)
    check_code(environ)
    if environ['instrument']:
        instrument_code(environ)
    else:
        compile_model(environ)


@env_options
@task
def prepare(environ, **kwargs):
    link_agcm_inputs(environ)
    prepare_workdir(environ)


@env_options
@task
def run(environ, **kwargs):
    run_model(environ)
