#!/usr/bin/env python

from __future__ import with_statement
from __future__ import print_function
import re

from fabric.api import run, cd, prefix
from fabric.decorators import task
import fabric.colors as fc

from environ import env_options, fmt, shell_env


@task
@env_options
def run_model(environ, **kwargs):
    ''' Submits coupled model

    Used vars:
      workdir
      platform
      walltime
      datatable
      diagtable
      fieldtable
      executable
      execdir
      TRUNC
      LEV
      LV
      rootexp
      mppnccombine
      comb_exe
      envconf
      expdir
      mode
      start
      restart
      finish
      npes
      name
      JobID_model

    Depends on:
      None
    '''
    print(fc.yellow('Submitting coupled model'))
    keys = ['workdir', 'platform', 'walltime', 'datatable', 'diagtable',
            'fieldtable', 'executable', 'execdir', 'TRUNC', 'LEV', 'LV',
            'rootexp', 'mppnccombine', 'comb_exe']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts', environ)):
                output = run(fmt('. run_g4c_model.cray {mode} {start} '
                                 '{restart} {finish} {npes} {name}', environ))
    environ['JobID_model'] = re.search(".*JobIDmodel:\s*(.*)\s*", output).groups()[0]


@task
@env_options
def compile_model(environ, **kwargs):
    keys = ['root', 'expdir', 'comp']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                #TODO: generate RUNTM and substitute
                run(fmt('make -f {makeconf}', environ))


