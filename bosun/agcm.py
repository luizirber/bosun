#!/usr/bin/env python

from __future__ import with_statement
from __future__ import print_function
from StringIO import StringIO
import re
from datetime import datetime

from fabric.api import run, cd, get, put, prefix, hide, settings
from fabric.decorators import task
import fabric.colors as fc
from fabric.contrib.files import exists
from mom_utils import nml_decode, yaml2nml

from bosun.environ import env_options, fmt, shell_env
from bosun.utils import total_seconds, JOB_STATES, print_ETA
from bosun.utils import hsm_full_path, clear_output


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

    Depends on:
      None
    '''
    input_file = StringIO()
    get(fmt('{agcm_namelist[file]}', environ), input_file)
    data = nml_decode(input_file.getvalue())
    input_file.close()
    output = StringIO()

    try:
        tkeys = set(
            environ['atmos_namelist']['vars'].keys()) & set(data.keys())
    except KeyError:
        pass
    else:
        for k in tkeys:
            keys = set(environ['atmos_namelist']['vars'][k]
                       .keys()) & set(data[k].keys())
            data[k].update([(
                ke, environ['atmos_namelist']['vars'][k][ke]) for ke in keys])

    trunc = "%04d" % environ['TRC']
    lev = "%03d" % environ['LV']

    data['MODEL_RES']['trunc'] = "%04d" % environ['TRC']
    data['MODEL_RES']['vert'] = environ['LV']
    data['MODEL_RES']['dt'] = environ['dt_atmos']
    data['MODEL_RES']['IDATEI'] = format_atmos_date(environ['start'])
    data['MODEL_RES']['IDATEW'] = format_atmos_date(environ['restart'])
    data['MODEL_RES']['IDATEF'] = format_atmos_date(environ['finish'])
    data['MODEL_RES']['DHEXT'] = environ.get('DHEXT', 0)
    if environ.get('DHEXT', 0) != 0:
        begin = datetime.strptime(environ['restart'], "%Y%m%d%H")
        end = datetime.strptime(environ['finish'], "%Y%m%d%H")
        nhext = total_seconds(end - begin) / 3600
    else:
        nhext = 0
    data['MODEL_RES']['NHEXT'] = nhext

    # TODO: is this environ['agcm_model_inputs'] ?
    data['MODEL_RES']['path_in'] = fmt(
        '{rootexp}/AGCM-1.0/model/datain', environ)

    data['MODEL_RES']['dirfNameOutput'] = (
        fmt('{workdir}/model/dataout/TQ%sL%s' % (trunc, lev), environ))

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
        out = run(
            fmt('qsub %s {workdir}/set_g4c_posgrib.cray' % opts, environ))
        environ['JobID_pos_atmos'] = out.split('\n')[-1]


@task
@env_options
def run_model(environ, **kwargs):
    ''' Submits atmos model

    Used vars:
      rootexp
      workdir
      executable
      walltime
      execdir
      platform
      TRC
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

    trunc = "%04d" % environ['TRC']
    lev = "%03d" % environ['LV']
    env_vars = environ.copy()
    env_vars.update({'TRUNC': trunc, 'LEV': lev})

    keys = ['rootexp', 'workdir', 'TRUNC', 'LEV', 'executable', 'walltime',
            'execdir', 'platform', 'LV']
    with shell_env(env_vars, keys=keys):
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
            with cd(fmt('{pre_atmos}/sources', environ)):
                fix_atmos_makefile()
                run(fmt('make cray', environ))


@task
@env_options
def compile_post(environ, **kwargs):
    with shell_env(environ, keys=['PATH2']):
        with prefix(fmt('source {envconf_pos}', environ)):
            with cd(environ['posgrib_src']):
                fix_atmos_makefile()
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
    run(fmt('mkdir -p {PATH2}', environ))
    link_agcm_inputs(environ)


@task
@env_options
def check_restart(environ, **kwargs):
    if 'warm' in environ['mode']:
        prepare_restart(environ)
        run(fmt('ls {workdir}/model/dataout/TQ{TRC:04d}L{LV:03d}/*{start}{restart}F.unf*outatt*', environ))


@task
@env_options
def prepare_inputs(environ, **kwargs):
    #TODO: copy data to pre/datain (look at oper experiment)
    with cd(fmt('pre_atmos/scripts', environ)):
        fix_atmos_runpre(environ)
        envvars = {
            'dirhome': fmt('{pre_atmos}', environ),
            'dirdata': fmt('{rootexp}/AGCM-1.0', environ),
            'direxe': fmt('{execdir}', environ)
        }
        with shell_env(envvars):
            run(fmt('bash/runAll.bash', environ))


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


def fix_atmos_runpre(environ):
    '''Stupid pre runscripts...'''
    run(fmt("sed -i.bak -r -e 's/^export DATA=.*$/export DATA={start}/g' bash/runAll.bash", environ))
    run(fmt("sed -i.bak -r -e 's|^export dirhome|#export dirhome|g' bash/runAll.bash", environ))
    run(fmt("sed -i.bak -r -e 's|^export dirdata|#export dirdata|g' bash/runAll.bash", environ))
#    for script in ls('bash/*.bash'):
#        run("sed -i.bak -r -e 's|^export direxe|#export direxe|g' %s" % script)
    #TODO: which parts of preprocessing to run? Comment all the vars in runAll,
    #and set as appropriate?


@task
@env_options
def clean_experiment(environ, **kwargs):
    run(fmt('rm -rf {PATH2}', environ))


def check_status(environ, status):
    if status['ID'] in environ.get('JobID_pos_atmos', ""):
        print(fc.yellow('Atmos post-processing: %s' % JOB_STATES[status['S']]))
    elif status['ID'] in environ.get('JobID_model', ""):
        if status['S'] == 'R':
            with hide('running', 'stdout'):
                fcts = run(fmt('find {workdir}/model/dataout/ -iname "*.fct.*" | sort', environ))
            date = re.search('.*(\d{10})F.*', fcts.splitlines()[-1])
            if date:
                current = datetime.strptime(date.groups()[0], "%Y%m%d%H")
                print_ETA(environ, status, current)
            else:
                print(fc.yellow('Preparing!'))
        else:
            print(fc.yellow('Model: %s' % JOB_STATES[status['S']]))


@task
@env_options
def archive(environ, **kwargs):
    full_path, cname = hsm_full_path(environ)

    run('mkdir -p %s/atmos/%s' % (full_path, cname))
    # TODO: copy AGCM output ({workdir}/pos/dataout)

    run('mkdir -p %s/output' % full_path)
    with cd(fmt('{workdir}', environ)):
        with settings(warn_only=True):
            out = run(fmt('ls -1 MODELIN Out.MPI.* set_post*out.txt '
                      'set_g4c_posgrib*out.txt POSTIN-GRIB '
                      'set_g4c_poseta*out.txt 2>/dev/null', environ))
        files = clear_output(out).splitlines()
        for f in files:
            if exists(f):
                if f not in ('MODELIN', 'POSTIN-GRIB'):
                    run(fmt('gzip %s' % f, environ))
                    f = f + '.gz'
                run(fmt('mv %s %s/output/' % (f, full_path), environ))

    run('mkdir -p %s/restart' % full_path)
    with cd(fmt('{workdir}/model/dataout/TQ{TRC:04}L{LV:03}', environ)):
        run(fmt('tar czvf GFCTNMC{start}{finish}F.unf.TQ{TRC:04}L{LV:03}.tar.gz '
            'GFCTNMC{start}{finish}F.unf.TQ{TRC:04}L{LV:03}.*P???', environ))
        run(fmt('mv GFCTNMC{start}{finish}F.unf.TQ{TRC:04}L{LV:03}.tar.gz '
            '%s/restart/' % full_path, environ))
        run(fmt('rm GFCTNMC{start}{finish}F.unf.TQ{TRC:04}L{LV:03}.*P???',
            environ))


@task
@env_options
def prepare_restart(environ, **kwargs):
    '''Prepare restart for new run'''
    with settings(warn_only=True):
        out = run(fmt('ls {workdir}/model/dataout/TQ{TRC:04d}L{LV:03d}/*{start}{restart}F.unf*outatt*', environ))
    if out.failed:
        with cd(fmt('{workdir}/model/dataout/TQ{TRC:04}L{LV:03}', environ)):
            full_path, cname = hsm_full_path(environ)
            run(fmt('tar xf %s/restart/GFCTNMC{start}{restart}F.unf.TQ{TRC:04}L{LV:03}.tar.gz' % full_path, environ))


@task
@env_options
def verify_run(environ, **kwargs):
    pass
