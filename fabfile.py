#!/usr/bin/env python

import fabric.colors as fc
from fabric.decorators import task

import tasks


@tasks.env_options
@task
def deploy(environ, instrument=False, **kwargs):
    print(fc.green("Started"))
    tasks.prepare_expdir(environ)
    tasks.check_code(environ)
    if instrument:
        tasks.instrument_code(environ)
    else:
        tasks.compile_model(environ)
    tasks.link_agcm_inputs(environ)
    tasks.prepare_workdir(environ)
    tasks.run_model(environ)
