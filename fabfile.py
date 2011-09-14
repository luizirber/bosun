#!/usr/bin/env python

import fabric.colors as fc
from fabric.decorators import task

from tasks import prepare_expdir, check_code, instrument_code, compile_model
from tasks import link_agcm_inputs, prepare_workdir, run_model, env_options
from tasks import link_agcm_pos_inputs


@env_options
@task
def deploy(environ, instrument=False, **kwargs):
    print(fc.green("Started"))
    prepare_expdir(environ)
    check_code(environ)
    if instrument:
        instrument_code(environ)
    else:
        compile_model(environ)
    link_agcm_inputs(environ)
    link_agcm_pos_inputs(environ)
    prepare_workdir(environ)
    run_model(environ)


@env_options
@task
def test_compilation(environ, instrument=False, **kwargs):
    print(fc.green("Started"))
    prepare_expdir(environ)
    check_code(environ)
    if instrument:
        instrument_code(environ)
    else:
        compile_model(environ)
