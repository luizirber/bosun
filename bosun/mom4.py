#!/usr/bin/env python

import sys
from StringIO import StringIO
import re
from datetime import datetime

from fabric.api import run, cd, get, put, prefix, settings, hide
from fabric.contrib.files import exists
from fabric.decorators import task
import fabric.colors as fc
from mom_utils import layout, nml_decode, yaml2nml

from bosun.environ import env_options, fmt, shell_env
from bosun.utils import print_ETA, JOB_STATES, hsm_full_path, clear_output


@task
@env_options
def prepare(environ, **kwargs):
    prepare_expdir(environ)
    prepare_workdir(environ)


@task
@env_options
def prepare_workdir(environ, **kwargs):
    '''Prepare output dir

    Used vars:
      workdir
      workdir_template
    '''
    print(fc.yellow('Preparing workdir'))
    run(fmt('mkdir -p {workdir}', environ))
    run(fmt('rsync -rtL --progress {workdir_template}/* {workdir}', environ))
    run(fmt('touch {workdir}/time_stamp.restart', environ))
    # TODO: lots of things.
    #  1) generate atmos inputs (gdas_to_atmos from oper scripts)
    #  2) copy restart files from somewhere (emanuel's spinup, for example)


@task
@env_options
def prepare_expdir(environ, **kwargs):
    run(fmt('mkdir -p {comb_exe}', environ))
    if environ.get('gengrid_run_this_module', False):
        run(fmt('mkdir -p {execdir}/gengrid', environ))
        run(fmt('mkdir -p {gengrid_workdir}', environ))
    if environ.get('make_xgrids_run_this_module', False):
        run(fmt('mkdir -p {execdir}/make_xgrids', environ))
        run(fmt('mkdir -p {make_xgrids_workdir}', environ))
    if environ.get('regrid_3d_run_this_module', False):
        run(fmt('mkdir -p {execdir}/regrid_3d', environ))
        run(fmt('mkdir -p {regrid_3d_workdir}', environ))
    if environ.get('regrid_2d_run_this_module', False):
        run(fmt('mkdir -p {execdir}/regrid_2d', environ))
        run(fmt('mkdir -p {regrid_2d_workdir}', environ))
    # Need to check if input.nml->ocean_drifters_nml->use_this_module is True
    run(fmt('mkdir -p {workdir}/DRIFTERS', environ))


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
    input_file = StringIO()
    get(fmt('{ocean_namelist[file]}', environ), input_file)
    data = nml_decode(input_file.getvalue())
    input_file.close()
    output = StringIO()

    try:
        tkeys = set(
            environ['ocean_namelist']['vars'].keys()) & set(data.keys())
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
        data['ice_model_nml']['layout'] = ("0,0")
    else:
        data['ocean_model_nml']['layout'] = ("%d,%d"
                                             % layout(int(environ['npes'])))
        data['ice_model_nml']['layout'] = ("%d,%d"
                                             % layout(int(environ['npes'])))

    data['ocean_model_nml']['dt_ocean'] = environ['dt_ocean']
    data['coupler_nml']['dt_atmos'] = environ['dt_atmos']
    data['coupler_nml']['dt_cpld'] = environ['dt_cpld']

    if 'days' in data['coupler_nml']:
        data['coupler_nml'].pop('days')
    if 'months' in data['coupler_nml']:
        data['coupler_nml'].pop('months')

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
    data['coupler_nml']['current_date'] = start.strftime(
        "%Y, %m, %d, %H, 0, 0")

    if 'ocean_drifters_nml' in data.keys():
        if data['ocean_drifters_nml']['use_this_module']:
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
        if environ.get('regrid_3d_run_this_module', False):
            with shell_env(environ, keys=['root', 'mkmf_template', 'executable_regrid_3d']):
                with cd(fmt('{execdir}/regrid_3d', environ)):
                    run(fmt('/usr/bin/tcsh {regrid_3d_makeconf}', environ))
        if environ.get('regrid_2d_run_this_module', False):
            with shell_env(environ, keys=['root', 'mkmf_template', 'executable_regrid_2d']):
                with cd(fmt('{execdir}/regrid_2d', environ)):
                    run(fmt('/usr/bin/tcsh {regrid_2d_makeconf}', environ))
    if environ.get('make_xgrids_run_this_module', False):
        with prefix(fmt('source {make_xgrids_envconf}', environ)):
            #run(fmt('cc -g -V -O -o {executable_make_xgrids} {make_xgrids_src} -I $NETCDF_DIR/include -L $NETCDF_DIR/lib -lnetcdf -lm -Duse_LARGEFILE -Duse_netCDF -DLARGE_FILE -Duse_libMPI', environ))
            fix_MAXLOCAL_make_xgrids(environ)
            run(fmt('cc -g -V -O -o {executable_make_xgrids} {make_xgrids_src} -I $NETCDF_DIR/include -L $NETCDF_DIR/lib -lnetcdf -lm -Duse_LARGEFILE -Duse_netCDF -DLARGE_FILE', environ))


def fix_MAXLOCAL_make_xgrids(environ):
    run(fmt("sed -i.bak -r -e 's/^#define MAXLOCAL.*$/#define MAXLOCAL 1e8/g' {make_xgrids_src}", environ))


@task
@env_options
def generate_grid(environ, **kwargs):
    run(fmt('cp {topog_file} {gengrid_workdir}/topog_file.nc', environ))
    with shell_env(
        environ, keys=['mom4_pre_npes', 'mom4_pre_walltime', 'RUNTM',
                       'executable_gengrid', 'gengrid_workdir',
                       'account', 'topog_file', 'platform']):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts/mom4_pre', environ)):
                out = run(fmt('/usr/bin/tcsh ocean_grid_run.csh', environ))


@task
@env_options
def regrid_3d(environ, **kwargs):
    run(fmt(
        'cp {regrid_3d_src_file} {regrid_3d_workdir}/src_file.nc', environ))
    with shell_env(environ, keys=['mom4_pre_npes', 'mom4_pre_walltime',
                                  'executable_regrid_3d', 'regrid_3d_workdir',
                                  'regrid_3d_dest_grid', 'regrid_3d_output_filename',
                                  'account', 'platform']):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts/mom4_pre', environ)):
                out = run(fmt('/usr/bin/tcsh regrid_3d_run.csh', environ))


@task
@env_options
def regrid_2d(environ, **kwargs):
    regrid_2d_prepare(environ)
    with shell_env(environ, keys=['mom4_pre_npes', 'mom4_pre_walltime',
                                  'executable_regrid_2d', 'regrid_2d_workdir',
                                  'regrid_2d_src_file', 'account', 'platform']):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts/mom4_pre', environ)):
                out = run(fmt('/usr/bin/tcsh regrid_2d_run.csh', environ))


@task
@env_options
def regrid_2d_prepare(environ, **kwargs):
    input_file = StringIO()
    get(fmt('{regrid_2d_namelist[file]}', environ), input_file)
    data = nml_decode(input_file.getvalue())
    input_file.close()
    output = StringIO()

    try:
        tkeys = set(
            environ['regrid_2d_namelist']['vars'].keys()) & set(data.keys())
    except KeyError:
        pass
    else:
        for k in tkeys:
            keys = (set(environ['regrid_2d_namelist']['vars'][k].keys())
                    & set(data[k].keys()))
            data[k].update([(ke, environ['regrid_2d_namelist']['vars'][k][ke])
                            for ke in keys])

    src_file = data['regrid_2d_nml']['src_file']
    run(fmt('cp %s {regrid_2d_workdir}/src_file.nc' % src_file, environ))
    data['regrid_2d_nml']['src_file'] = 'src_file.nc'
    output.write(yaml2nml(data))

    put(output, fmt('{regrid_2d_workdir}/input.nml', environ))
    output.close()


@task
@env_options
def make_xgrids(environ, **kwargs):
    with prefix(fmt('source {envconf}', environ)):
        with cd(fmt('{workdir}/gengrid', environ)):
            with settings(warn_only=True):
                out = run(fmt('{executable_make_xgrids} -o ocean_grid.nc -a {atmos_gridx},{atmos_gridy}', environ))
            # TODO: need to check why it returns 41 even if program ended right.
            # An appropriate return code is missing in make_xgrids.c ...
            if out.return_code == 41:
                run(fmt('cp ocean_grid.nc grid_spec_UNION.nc', environ))
                if exists(fmt('{workdir}/gengrid/ocean_grid?.nc', environ)):
                    run(fmt('for file in ocean_grid?.nc; do ncks -A $file grid_spec_UNION.nc; done', environ))
                run(fmt('ncks -A grid_spec.nc grid_spec_UNION.nc', environ))
                if exists(fmt('{workdir}/gengrid/grid_spec?.nc', environ)):
                    run(fmt('for file in grid_spec?.nc; do ncks -A $file grid_spec_UNION.nc; done', environ))
            else:
                sys.exit(1)


def get_coupler_dates(environ):
    res_time = run(fmt('tail -1 {workdir}/INPUT/coupler.res', environ))
    date_comp = [i for i in res_time.split('\n')[-1].split(' ') if i][:4]
    res_date = int("".join(
        date_comp[0:1] + ["%02d" % int(i) for i in date_comp[1:4]]))

    if 'cold' in environ['mode']:
        cmp_date = int(environ['start'])
    else:
        cmp_date = int(environ['restart'])

    return res_date, cmp_date


@task
@env_options
def check_restart(environ, **kwargs):
    prepare_restart(environ)
    if exists(fmt('{workdir}/INPUT/coupler.res', environ)):
        res_date, cmp_date = get_coupler_dates(environ)

        if res_date != cmp_date:
            print(fc.red('ERROR'))
            sys.exit(1)
    else:
        # TODO: check if it starts from zero (ocean forced)
        sys.exit(1)


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
                if environ.get('run_drifters_pos', False):
                    run(fmt('. set_pos_drifters.cray', environ))
                output = run(fmt('. run_g4c_model.cray {mode} {start} '
                                 '{restart} {finish} {npes} {name}', environ))
    environ['JobID_model'] = re.search(
        ".*JobIDmodel:\s*(.*)\s*", output).groups()[0]


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
        out = run(fmt(
            'qsub %s {workdir}/set_g4c_pos_m4g4.{platform}' % opts, environ))
        environ['JobID_pos_ocean'] = out.split('\n')[-1]

        if environ.get('run_drifters_pos', False):
            out = run(fmt('qsub %s {workdir}/run_pos_drifters.{platform}' %
                      opts, environ))
            environ['JobID_pos_ocean'] = out.split('\n')[-1]


@task
@env_options
def clean_experiment(environ, **kwargs):
    run(fmt('rm -rf {comb_exe}', environ))


def check_status(environ, status):
    if status['ID'] in environ.get('JobID_pos_ocean', ""):
        print(fc.yellow('Ocean post-processing: %s' % JOB_STATES[status['S']]))
    elif status['ID'] in environ.get('JobID_model', ""):
        fmsfile = fmt("{workdir}/fms.out", environ)
        if status['S'] == 'R':
            if not exists(fmsfile):
                print(fc.yellow('Preparing!'))
            else:
                with hide('running', 'stdout'):
                    line = run('tac %s | grep -m1 yyyy' % fmsfile).splitlines()[-1]
                current = re.search('(\d{4})/(\s*\d{1,2})/(\s*\d{1,2})\s(\s*'
                                    '\d{1,2}):(\s*\d{1,2}):(\s*\d{1,2})', line)
                if current:
                    current = datetime(*[int(i) for i in current.groups()])
                    print_ETA(environ, status, current)
                else:
                    print(fc.yellow('Preparing!'))
        else:
            print(fc.yellow('Model: %s' % JOB_STATES[status['S']]))


@task
@env_options
def archive(environ, **kwargs):
    full_path, cname = hsm_full_path(environ)

    run('mkdir -p %s/ocean/%s' % (full_path, cname))
    # TODO: copy OGCM output ({workdir}/dataout)

    run('mkdir -p %s/output' % full_path)
    with cd(fmt('{workdir}', environ)):
        with settings(warn_only=True):
            out = run(fmt('ls -1 *fms.out *logfile.*.out input.nml '
                      'set_g4c_model*out.txt *_table *diag_integral.out '
                      'set_g4c_pos_m4g4*out.txt *time_stamp.out 2> /dev/null', environ))
        files = clear_output(out).splitlines()
        for f in files:
            if exists(f):
                if f not in ('data_table', 'diag_table', 'field_table', 'input.nml'):
                    run(fmt('gzip %s' % f, environ))
                    f = f + '.gz'
                run(fmt('mv %s %s/output/' % (f, full_path), environ))

    run('mkdir -p %s/restart' % full_path)
    with cd(fmt('{workdir}/RESTART', environ)):
        # TODO: check date in coupler.res!
        run(fmt('tar czvf {finish}.tar.gz coupler* ice* land* ocean*', environ))
        run(fmt('mv {finish}.tar.gz %s/restart/' % full_path, environ))
    with cd(fmt('{workdir}', environ)):
        run(fmt('tar czvf INPUT.tar.gz INPUT/ --exclude="*.res*"', environ))
        run(fmt('mv INPUT.tar.gz %s/restart/' % full_path, environ))


@task
@env_options
def prepare_restart(environ, **kwargs):
    '''Prepare restart for new run'''

    # TODO: check if it starts from zero (ocean forced)

    if 'cold' in environ['mode']:
        cmp_date = str(environ['start'])
    else:
        cmp_date = str(environ['restart'])

    with settings(warn_only=True):
        with cd(fmt('{workdir}/INPUT', environ)):
            full_path, cname = hsm_full_path(environ)
            run(fmt('tar xf %s/restart/%s.tar.gz' % (full_path, cmp_date), environ))


@task
@env_options
def verify_run(environ, **kwargs):
    if 'cold' in environ['mode']:
        cmp_date = str(environ['start'])
    else:
        cmp_date = str(environ['restart'])
    run(fmt('grep "Total runtime" {workdir}/%s.fms.out' % cmp_date[:8], environ))
    # TODO: need to check post processing!
