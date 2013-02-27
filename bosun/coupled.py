#!/usr/bin/env python

from __future__ import with_statement
from __future__ import print_function
import re

from fabric.api import run, cd, prefix
from fabric.decorators import task
import fabric.colors as fc

from bosun import mom4, agcm
from bosun.environ import env_options, fmt, shell_env


__all__ = ['compile_model', 'run_model', 'prepare', 'compile_pre', 'compile_post']


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
      TRC
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

    trunc = "%04d" % environ['TRC']
    lev = "%03d" % environ['LV']
    env_vars = environ.copy()
    env_vars.update({'TRUNC': trunc, 'LEV': lev})

    keys = ['workdir', 'platform', 'walltime', 'datatable', 'diagtable',
            'fieldtable', 'executable', 'execdir', 'TRUNC', 'LEV', 'LV',
            'rootexp', 'mppnccombine', 'comb_exe', 'account', 'DHEXT']
    with shell_env(env_vars, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts', environ)):
                output = run(fmt('. run_g4c_model.cray {mode} {start} '
                                 '{restart} {finish} {npes} {name}', environ))
    environ['JobID_model'] = re.search(".*JobIDmodel:\s*(.*)\s*", output).groups()[0]


@task
@env_options
def compile_model(environ, **kwargs):
    keys = ['comp', 'code_dir', 'root', 'type', 'mkmf_template', 'executable']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                run(fmt('/usr/bin/tcsh -e {cpld_makeconf}', environ))


@task
@env_options
def prepare(environ, **kwargs):
    mom4.prepare_expdir(environ)
    mom4.prepare_workdir(environ)
    agcm.link_agcm_inputs(environ)


@task
@env_options
def compile_pre(environ, **kwargs):
    mom4.compile_pre(environ)
    agcm.compile_pre(environ)


@task
@env_options
def compile_post(environ, **kwargs):
    mom4.compile_post(environ)
    agcm.compile_post(environ)


@task
@env_options
def run_post(environ, **kwargs):
    mom4.run_post(environ)
    agcm.run_post(environ)
