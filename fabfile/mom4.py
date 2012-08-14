#!/usr/bin/env python

import os.path
from StringIO import StringIO
import re
from datetime import datetime

from fabric.api import run, local, cd, lcd, get, put, prefix
from fabric.decorators import task
import fabric.colors as fc
from mom4_utils import layout, nml_decode, yaml2nml

from environ import env_options, fmt, shell_env


@task
@env_options
def prepare_namelist(environ, **kwargs):
    ''' Read ocean namelist and update variables from environ as needed

    Used vars:
      name
      ocean_namelist
      npes
      dt_ocean
      dt_atmos
      dt_cpld
      days
      workdir

    Depends on:
      None
    '''
    exp_workspace = fmt('workspace/{name}', environ)
    local('mkdir -p %s' % exp_workspace)
    with lcd(exp_workspace):
        get(fmt('{ocean_namelist[file]}', environ), 'input.nml')
    namelist = open(os.path.join(exp_workspace, 'input.nml')).read()
    data = nml_decode(namelist)
    output = StringIO()

    try:
        tkeys = set(environ['ocean_namelist']['vars'].keys()) & set(data.keys())
    except KeyError:
        pass
    else:
        for k in tkeys:
            keys = (set(environ['ocean_namelist']['vars'][k].keys())
                  & set(data[k].keys()))
            data[k].update([(ke, environ['ocean_namelist']['vars'][k][ke])
                            for ke in keys])

    if data['coupler_nml'].get('concurrent', False):
        data['ocean_model_nml']['layout'] = ("%d,%d"
                               % layout(data['coupler_nml']['ocean_npes']))
    else:
        data['ocean_model_nml']['layout'] = ("%d,%d"
                               % layout(int(environ['npes'])))

    data['ocean_model_nml']['dt_ocean'] = environ['dt_ocean']
    data['coupler_nml']['dt_atmos'] = environ['dt_atmos']
    data['coupler_nml']['dt_cpld'] = environ['dt_cpld']
    if ('days' in environ) & ('months' not in environ):
        data['coupler_nml']['days'] = environ['days']
    elif ('days' not in environ) & ('months' in environ):
        data['coupler_nml']['months'] = environ['months']
    else:
        print "Error, one should use days or months, not both or none"
    if environ['mode'] == 'warm':
        start = datetime.strptime(str(environ['restart']), "%Y%m%d%H")
    else:
        start = datetime.strptime(str(environ['start']), "%Y%m%d%H")
    data['coupler_nml']['current_date'] = start.strftime("%Y, %m, %d, %H, 0, 0")

    if 'ocean_drifters_nml' in data.keys():
        if data['ocean_drifters_nml']['use_this_module'] == True:
            environ['run_drifters_pos'] = True


    output.write(yaml2nml(data))

    put(output, fmt('{workdir}/input.nml', environ))
    output.close()


@task
@env_options
def compile_model(environ, **kwargs):
    keys = ['comp', 'code_dir', 'root', 'type', 'mkmf_template', 'executable']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                run(fmt('/usr/bin/tcsh {ocean_makeconf}', environ))


@task
@env_options
def compile_post(environ, **kwargs):
    with shell_env(environ, keys=['root', 'platform']):
        with prefix(fmt('source {envconf}', environ)):
            with cd(environ['comb_exe']):
                run(fmt('make -f {comb_src}/Make_combine', environ))
    run(fmt('cp {root}/MOM4p1/src/shared/drifters/drifters_combine {comb_exe}/', environ))


@task
@env_options
def compile_pre(environ, **kwargs):
    with prefix(fmt('source {envconf}', environ)):
        if environ.get('gengrid_run_this_module', False):
            with shell_env(environ, keys=['root', 'platform', 'mkmf_template', 'executable_gengrid']):
                with cd(fmt('{execdir}/gengrid', environ)):
                    run(fmt('/usr/bin/tcsh {gengrid_makeconf}', environ))
        if environ.get('make_xgrids_run_this_module', False):
            with cd(fmt('{execdir}/make_xgrids', environ)):
                run(fmt('cc -g -V -O -o {executable_make_xgrids} {make_xgrids_src} -I $NETCDF_DIR/include -L $NETCDF_DIR/lib -lnetcdf -lm', environ))


@task
@env_options
def generate_grid(environ, **kwargs):
    run(fmt('cp {topog_file} {gengrid_workdir}/topog_file.nc', environ))
    with shell_env(environ, keys=['gengrid_npes', 'gengrid_walltime', 'RUNTM', 'executable_gengrid',
                                  'gengrid_workdir', 'account', 'topog_file', 'platform']):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts', environ)):
                out = run(fmt('/usr/bin/tcsh ocean_grid_run.csh', environ))


@task
@env_options
def make_xgrids(environ, **kwargs):
    pass
#    run(fmt('cp {topog_file} {gengrid_workdir}/topog_file.nc', environ))
#    with shell_env(environ, keys=['gengrid_npes', 'gengrid_walltime', 'RUNTM', 'executable_gengrid',
#                                  'gengrid_workdir', 'account', 'topog_file', 'platform']):
#        with prefix(fmt('source {envconf}', environ)):
#            with cd(fmt('{expdir}/runscripts', environ)):
#                out = run(fmt('/usr/bin/tcsh ocean_grid_run.csh', environ))


@task
@env_options
def run_model(environ, **kwargs):
    ''' Submits ocean model

    Used vars:
      workdir
      platform
      walltime
      datatable
      diagtable
      fieldtable
      executable
      execdir
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
    print(fc.yellow('Submitting ocean model'))

    # Here goes a series of tests and preparations moved out from the
    #   mom4p1_coupled_run.csh, that are better be done here.
    # For some reason, this line bellow is not working. The dir does exist
    #   and this gives the error message, and do not stop here with the return.
    # Didn't understand.

    #if not exists(fmt('{workdir}/INPUT', environ)):
    #    print(fc.yellow(fmt("Missing the {workdir}/INPUT directory!", environ)))
    #    return
    #if not exists(fmt('{workdir}', environ)):
    #    print(fc.yellow(fmt("Missing the {workdir} directory!", environ)))
    #    run(fmt('mkdir -p {workdir}', environ))
    #if not exists(fmt('{workdir}/RESTART', environ)):
    #    print(fc.yellow(fmt("Missing the {workdir}/INPUT directory!", environ)))
    #    run(fmt('mkdir -p {workdir}/RESTART', environ))
    #if not exists(fmt('{workdir}/INPUT/grid_spec.nc', environ)):
    #    print(fc.yellow(fmt("ERROR: required input file does not exist {workdir}/INPUT/grid_spec.nc", environ)))
    #    return
    #if not exists(fmt('{workdir}/INPUT/ocean_temp_salt.res.nc', environ)):
    #    print(fc.yellow(fmt("ERROR: required input file does not exist {workdir}/INPUT/ocean_temp_salt.res.nc", environ)))
    #    return
    #run(fmt('cp {ocean_namelist} {workdir}/input.nml', environ))
    #run(fmt('cp {datatable} {workdir}/data_table', environ))
    #run(fmt('cp {diagtable} {workdir}/diag_table', environ))
    #run(fmt('cp {fieldtable} {workdir}/field_table', environ))

    keys = ['workdir', 'platform', 'walltime', 'datatable', 'diagtable',
            'fieldtable', 'executable', 'mppnccombine', 'comb_exe',
            'account']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts', environ)):
                if environ.get('run_drifters_pos', False) == True:
                    run(fmt('. set_pos_drifters.cray', environ))
                output = run(fmt('. run_g4c_model.cray {mode} {start} '
                                 '{restart} {finish} {npes} {name}', environ))
    environ['JobID_model'] = re.search(".*JobIDmodel:\s*(.*)\s*",output).groups()[0]


@task
@env_options
def run_post(environ, **kwargs):
    ''' Submits ocean post-processing

    Used vars:
      JobID_model
      expdir
      workdir
      platform

    Depends on:
      None
    '''
    print(fc.yellow('Submitting ocean post-processing'))
    opts = ''
    if environ['JobID_model']:
        opts = '-W depend=afterok:{JobID_model}'
    with cd(fmt('{expdir}/runscripts', environ)):
        out = run(fmt('qsub %s {workdir}/set_g4c_pos_m4g4.{platform}' % opts, environ))
        environ['JobID_pos_ocean'] = out.split('\n')[-1]

        if environ.get('run_drifters_pos', False) == True:
            out = run(fmt('qsub %s {workdir}/run_pos_drifters.{platform}' % opts, environ))
            environ['JobID_pos_ocean'] = out.split('\n')[-1]
