#!/usr/bin/env python

from __future__ import with_statement
from __future__ import print_function
import os.path
from StringIO import StringIO
import re

from fabric.api import run, local, cd, lcd, get, put, prefix
from fabric.decorators import task
import fabric.colors as fc
from fabric.contrib.files import exists
from mom4_utils import nml_decode, yaml2nml

from fabfile.environ import env_options, fmt, shell_env


def format_atmos_date(date):
    ''' Format date as required by atmos model '''
    out = str(date)
    return "%s,%s,%s,%s" % (out[8:], out[6:8], out[4:6], out[0:4])


@task
@env_options
def prepare_namelist(environ, **kwargs):
    ''' Read atmos namelist and update variables from environ as needed

    Used vars:
      name
      atmos_namelist
      TRC
      LV
      dt_atmos
      start
      restart
      finish
      rootexp
      workdir
      TRUNC
      LEV

    Depends on:
      None
    '''
    exp_workspace = fmt('workspace/{name}', environ)
    local('mkdir -p %s' % exp_workspace)
    with lcd(exp_workspace):
        get(fmt('{agcm_namelist[file]}', environ), 'MODELIN')
    namelist = open(os.path.join(exp_workspace, 'MODELIN')).read()
    data = nml_decode(namelist)
    output = StringIO()

    try:
        tkeys = set(environ['atmos_namelist']['vars'].keys()) & set(data.keys())
    except KeyError:
        pass
    else:
        for k in tkeys:
            keys = set(environ['atmos_namelist']['vars'][k].keys()) & set(data[k].keys())
            data[k].update([(ke, environ['atmos_namelist']['vars'][k][ke]) for ke in keys])

    data['MODEL_RES']['trunc'] = "%04d" % environ['TRC']
    data['MODEL_RES']['vert'] = environ['LV']
    data['MODEL_RES']['dt'] = environ['dt_atmos']
    data['MODEL_RES']['IDATEI'] = format_atmos_date(environ['start'])
    data['MODEL_RES']['IDATEW'] = format_atmos_date(environ['restart'])
    data['MODEL_RES']['IDATEF'] = format_atmos_date(environ['finish'])

    # TODO: is this environ['agcm_model_inputs'] ?
    data['MODEL_RES']['path_in'] = fmt('{rootexp}/AGCM-1.0/model/datain', environ)

    data['MODEL_RES']['dirfNameOutput'] = (
        fmt('{workdir}/model/dataout/TQ{TRUNC}L{LEV}', environ))

    output.write(yaml2nml(data,
        key_order=['MODEL_RES', 'MODEL_IN', 'PHYSPROC',
                   'PHYSCS', 'COMCON']))

    # HACK: sigh, this is needed to run atmos post processing, even if we
    # don't use these forecasts.
    output.write("""
 17
   6.0 12.0  18.0  24.0
  30.0 36.0  42.0  48.0
  54.0 60.0  66.0  72.0
  84.0 96.0 120.0 144.0
 168.0
""")

    put(output, fmt('{workdir}/MODELIN', environ))
    output.close()


@task
@env_options
def run_post(environ, **kwargs):
    ''' Submits atmos post-processing

    Used vars:
      JobID_model
      expdir
      workdir
      platform

    Depends on:
      None
    '''
    print(fc.yellow('Submitting atmos post-processing'))
    opts = ''
    if environ['JobID_model']:
        opts = '-W depend=afterok:{JobID_model}'
    with cd(fmt('{expdir}/runscripts', environ)):
        out = run(fmt('qsub %s {workdir}/set_g4c_posgrib.cray' % opts, environ))
        environ['JobID_pos_atmos'] = out.split('\n')[-1]


@task
@env_options
def run_model(environ, **kwargs):
    ''' Submits atmos model

    Used vars:
      rootexp
      workdir
      TRUNC
      LEV
      executable
      walltime
      execdir
      platform
      LV
      envconf
      expdir
      start
      restart
      finish
      npes
      name
      JobID_model

    Depends on:
      None
    '''
    print(fc.yellow('Submitting atmos model'))
    keys = ['rootexp', 'workdir', 'TRUNC', 'LEV', 'executable', 'walltime',
            'execdir', 'platform', 'LV']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts', environ)):
                output = run(fmt('. run_atmos_model.cray run {start} {restart} '
                                 '{finish} {npes} {name}', environ))
    job_id = re.search(".*JobIDmodel:\s*(.*)\s*", output).groups()[0]
    environ['JobID_model'] = job_id


@task
@env_options
def compile_pre(environ, **kwargs):
    with shell_env(environ, keys=['PATH2']):
        with prefix(fmt('source {envconf_pos}', environ)):
            with cd(environ['preatmos_src']):
                fix_atmos_makefile(environ)
                run(fmt('make cray', environ))


@task
@env_options
def compile_post(environ, **kwargs):
    with shell_env(environ, keys=['PATH2']):
        with prefix(fmt('source {envconf_pos}', environ)):
            with cd(environ['posgrib_src']):
                fix_atmos_makefile(environ)
                run(fmt('make cray', environ))


@task
@env_options
def compile_model(environ, **kwargs):
    with shell_env(environ, keys=['root', 'executable']):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                run(fmt('make -f {atmos_makeconf}', environ))


@task
@env_options
def link_agcm_inputs(environ, **kwargs):
    '''Copy AGCM inputs for model run and post-processing to the right place

    Used vars:
      rootexp
      agcm_pos_inputs
      agcm_model_inputs
    '''
    for comp in ['model', 'pos']:
        print(fc.yellow(fmt("Linking AGCM %s input data" % comp, environ)))
        if not exists(fmt('{rootexp}/AGCM-1.0/%s/datain' % comp, environ)):
            run(fmt('mkdir -p {rootexp}/AGCM-1.0/%s/datain' % comp, environ))
        run(fmt('cp -R {agcm_%s_inputs}/* '
                '{rootexp}/AGCM-1.0/%s/datain' % (comp, comp), environ))


def fix_atmos_makefile():
    '''Stupid pre and pos makefiles...'''
    run("sed -i.bak -r -e 's/^PATH2/#PATH2/g' Makefile")
