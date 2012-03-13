#!/usr/bin/env python

from __future__ import with_statement
from __future__ import print_function
import re
import os.path
import functools
from pipes import quote
from pprint import pprint
import time
from datetime import timedelta, datetime
from StringIO import StringIO
import sys
from copy import deepcopy

from fabric.api import run, cd, prefix, put, get, lcd, local, settings, hide
import fabric.colors as fc
from fabric.contrib.files import exists
from fabric.decorators import task
import yaml
from dateutil.relativedelta import relativedelta
from mom4_utils import layout, nml_decode, yaml2nml


JOB_STATES = {
    'B': 'Array job has at least one subjob running.',
    'E': 'Job is exiting after having run.',
    'F': 'Job is finished.',
    'H': 'Job is held.',
    'M': 'Job was moved to another server.',
    'Q': 'Job is queued.',
    'R': 'Job is running.',
    'S': 'Job is suspended.',
    'T': 'Job is being moved to new location.',
    'U': 'Cycle-harvesting job is suspended due to keyboard activity.',
    'W': 'Job is waiting for its submitter-assigned start time to be reached.',
    'X': 'Subjob has completed execution or has been deleted.'
}


GET_STATUS_SLEEP_TIME = 60


__all__ = ['prepare_expdir', 'check_code', 'instrument_code', 'compile_model',
           'link_agcm_inputs', 'prepare_workdir', 'run_model', 'prepare_restart',
           'env_options', 'check_status', 'clean_experiment', 'kill_experiment',
           'compile_atmos_pre', 'compile_atmos_pos', 'compile_ocean_pos',
           'compile_coupled_model']


class NoEnvironmentSetException(Exception):
    pass


def genrange(*args):
    if len(args) != 3:
        return range(*args)
    start, stop, step = args
    if start < stop:
        cmp = lambda a, b: a < b
        inc = lambda a: a + step
    else:
        cmp = lambda a, b: a > b
        inc = lambda a: a - step
    output = []
    while cmp(start, stop):
        output.append(start)
        start = inc(start)
    return output


def total_seconds(td):
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6


def format_atmos_date(d):
    s = str(d)
    return "%s,%s,%s,%s" % (s[8:], s[6:8], s[4:6], s[0:4])


def env_options(f):
    '''Decorator that loads a YAML configuration file and expands
    cross-references.

    An example:
      workdir = ${HOME}/teste
      expdir = ${workdir}/exp
    would expand into:
      workdir = ${HOME}/teste
      expdir = ${HOME}/teste/exp
    The idea is: if a variable is not available to be replaced leave it there
    and the shell env should provide it.

    If an environ is passed for the original function it is used, if not the
    wrapper search for keyword argument 'config_file' and read the
    config file. Next step is to replace all the cross-referenced vars and then
    call the original function.
    '''

    def _wrapped_env(*args, **kw):
        if args:
            environ = args[0]
        else:
            try:
                exp_repo = kw.get('exp_repo', '${ARCHIVE_OCEAN}/exp_repos')
                name = kw['name']
            except KeyError:
                raise NoEnvironmentSetException

            environ = {'exp_repo': exp_repo,
                       'name': name,
                       'expfiles': 'autobot_exps'}
            with hide('running', 'stdout', 'stderr', 'warnings'):
                local('mkdir -p workspace')
                with lcd('workspace'):
                    if not exists(fmt('{expfiles}', environ)):
                        run(fmt('hg clone {exp_repo} {expfiles}', environ))
                    else:
                        with cd(fmt('{expfiles}', environ)):
                            run('hg pull')
                            run('hg update')

                    get(fmt('{expfiles}/exp/{name}/namelist.yaml', environ),
                        'exp.yaml')


            kw['expfiles'] = environ['expfiles']
            environ = _read_config('workspace/exp.yaml')
            environ = _expand_config_vars(environ, updates=kw)
            if environ is None:
                raise NoEnvironmentSetException
            else:
                if environ.get('ensemble', False):
                    environs = []
                    ensemble = environ['ensemble']
                    environ.pop('ensemble')
                    for member in ensemble.keys():
                        new_env = update_environ(environ, ensemble, member)
                        environs.append(new_env)

                    # TODO: ignoring for now, but need to think on how to run
                    # multiple environs (one for each ensemble member).
                    # Maybe use the Fabric JobQueue, which abstracts
                    # multiprocessing?

        return f(environ, **kw)

    return functools.update_wrapper(_wrapped_env, f)


def update_component(new_env, nml, comp):
    nml_vars = nml.get('vars', False)
    if nml_vars:
        if new_env[comp].get('vars', False):
            for k in nml_vars.keys():
                if new_env[comp]['vars'].get(k, None):
                    new_env[comp]['vars'][k].update(nml_vars[k])
                else:
                    new_env[comp]['vars'][k] = nml_vars[k]
        else:
            new_env[comp]['vars'] = nml_vars
        nml.pop('vars')

    new_env[comp].update(nml)
    return new_env


def update_environ(environ, ensemble, member):
    new_env = deepcopy(environ)

    ocean_nml = ensemble[member].get('ocean_namelist', False)
    if ocean_nml:
        new_env = update_component(new_env, ocean_nml, 'ocean_namelist')
        ensemble[member].pop('ocean_namelist')

    atmos_nml = ensemble[member].get('agcm_namelist', False)
    if atmos_nml:
        new_env = update_component(new_env, atmos_nml, 'agcm_namelist')
        ensemble[member].pop('agcm_namelist')

    new_env['name'] = member
    new_env.update(ensemble[member])

    return new_env


def shell_env(environ, keys=None):
    '''Context manager that can send shell env variables to remote side.

    Really dumb context manager for Fabric, in fact. It just generates a new
    prefix with an export before the actual command. Something like

    $ export workdir=${HOME}/teste expdir=${HOME}/teste/exp && <cmd>
    '''

    if not keys:
        keys = environ.keys()
    valid = [k for k in keys
             if not isinstance(environ[k], (dict, list, tuple))]

    env_vars = " ".join(["=".join((key, str(value)))
                                   for (key, value) in environ.items()
                                   if key in valid])
    return prefix("export %s" % env_vars)


def fmt(s, e):
    '''String formatting and sanitization function'''
    return s.format(**e)


def _expand_config_vars(d, updates=None):

    env_vars = {}

    def rec_replace(env, value):
        finder = re.compile('\$\{(\w*)\}')
        ret_value = value

        try:
            keys = finder.findall(value)
        except TypeError:
            return value

        for k in keys:
            if k in env:
                out = env[k]
            else:
                if k not in env_vars:
                    with hide('running', 'stdout', 'stderr', 'warnings'):
                        env_vars[k] = run('echo $%s' % k)
                out = env_vars[k]

            ret_value = rec_replace(env, ret_value.replace('${%s}' % k, out))

        return ret_value

    def env_replace(old_env, ref=None):
        new_env = {}
        for k in old_env.keys():
            try:
                # tests if value is a string
                old_env[k].split(' ')
            except AttributeError:
                # if it's not a string, let's test if it is a dict
                try:
                    old_env[k].keys()
                except AttributeError:
                    # if it's not, just set new_env with it
                    new_env[k] = old_env[k]
                else:
                    # Yup, a dict. Need to replace recursively too.
                    new_env[k] = env_replace(old_env[k], old_env)
            else:
                # if it is, start replacing vars
                new_env[k] = rec_replace(ref if ref else old_env, old_env[k])
        return new_env

    if updates:
        d.update(updates)
    new_d = env_replace(d)

    # FIXME: sanitize input!
    return new_d


def _read_config(filename):
    data = open(filename, 'r').read()
    d = yaml.load(data)
    return d


@env_options
@task
def instrument_code(environ, **kwargs):
    '''Instrument code using Cray's perftools.

    Used vars:
      executable
      expdir

    Depends on:
      clean_model_compilation
      compile_model
    '''
    print(fc.yellow('Rebuilding executable with instrumentation'))
    with prefix('module load perftools'):
        #clean_model_compilation(environ)
        compile_model(environ)
        if exists(fmt('{executable}+apa', environ)):
            run(fmt('rm {executable}+apa', environ))
        run(fmt('pat_build -O {expdir}/instrument_coupler.apa '
                '-o {executable}+apa {executable}', environ))
        environ['executable'] = fmt('{executable}+apa', environ)


@env_options
@task
def prepare_ocean_namelist(environ, **kwargs):
    exp_workspace = fmt('workspace/{name}', environ)
    local('mkdir -p %s' % exp_workspace)
    with lcd(exp_workspace):
        get(fmt('{ocean_namelist[file]}', environ), 'input.nml')
    namelist = open(os.path.join(exp_workspace, 'input.nml')).read()
    data = nml_decode(namelist)
    output = StringIO()

#    keys = set(environ.keys()) & set(data.keys())
#    data.update([(k, environ[k]) for k in keys])

    data['ocean_model_nml']['layout'] = "%d,%d" % layout(int(environ['npes']))
    data['ocean_model_nml']['dt_ocean'] = environ['dt_ocean']
    data['coupler_nml']['dt_atmos'] = environ['dt_atmos']
    data['coupler_nml']['dt_cpld'] = environ['dt_cpld']
    data['coupler_nml']['months'] = environ['months']
    data['coupler_nml']['days'] = environ['days']

    output.write(yaml2nml(data))

    put(output, fmt('{workdir}/input.nml', environ))
    output.close()


@env_options
@task
def prepare_atmos_namelist(environ, **kwargs):
    exp_workspace = fmt('workspace/{name}', environ)
    local('mkdir -p %s' % exp_workspace)
    with lcd(exp_workspace):
        get(fmt('{agcm_namelist[file]}', environ), 'MODELIN')
    namelist = open(os.path.join(exp_workspace, 'MODELIN')).read()
    data = nml_decode(namelist)
    output = StringIO()

#    try:
#        nml_vars = environ['agcm_namelist']['vars']
#    except KeyError:
#        environ['agcm_namelist']['vars'] = {}
#    env_keys = environ['agcm_namelist']['vars'].keys()
#
#    keys = set(env_keys) & set(data.keys())
#    data.update([(k, environ['agcm_namelist']['vars'][k]) for k in keys])

    data['MODEL_RES']['trunc'] = "%04d" % environ['TRC']
    data['MODEL_RES']['vert'] = environ['LV']
    data['MODEL_RES']['dt'] = environ['dt_atmos']
    data['MODEL_RES']['IDATEI'] = format_atmos_date(environ['start'])
    data['MODEL_RES']['IDATEW'] = format_atmos_date(environ['restart'])
    data['MODEL_RES']['IDATEF'] = format_atmos_date(environ['finish'])

    # TODO: is this environ['agcm_model_inputs'] ?
    data['MODEL_RES']['path_in'] = fmt('{rootexp}/AGCM-1.0/model/datain', environ)

    data['MODEL_RES']['dirfNameOutput'] = fmt('{workdir}/model/dataout/TQ{TRUNC}L{LEV}', environ)

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


@env_options
@task
def run_pos_atmos(environ, **kwargs):
    print(fc.yellow('Submitting atmos post-processing'))
    opts = ''
    if environ['JobID_model']:
        opts = '-W depend=afterok:{JobID_model}'
    with cd(fmt('{expdir}/runscripts', environ)):
        environ['JobID_pos_atmos'] = run(
            fmt('qsub %s {workdir}/set_g4c_posgrib.cray' % opts, environ))


@env_options
@task
def run_pos_ocean(environ, **kwargs):
    print(fc.yellow('Submitting ocean post-processing'))
    opts = ''
    if environ['JobID_model']:
        opts = '-W depend=afterok:{JobID_model}'
    with cd(fmt('{expdir}/runscripts', environ)):
        environ['JobID_pos_ocean'] = run(
            fmt('qsub %s {workdir}/set_g4c_pos_m4g4.{platform}' % opts, environ))


@env_options
@task
def run_atmos_model(environ, **kwargs):
    print(fc.yellow('Submitting atmos model'))
    keys = ['rootexp', 'workdir', 'TRUNC', 'LEV', 'executable', 'walltime',
            'execdir', 'platform', 'LV']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts', environ)):
                output = run(fmt('. run_atmos_model.cray run {start} {restart} '
                                 '{finish} {npes} {name}', environ))
    environ['JobID_model'] = re.search(".*JobIDmodel:\s*(.*)\s*",output).groups()[0]


@env_options
@task
def run_coupled_model(environ, **kwargs):
    print(fc.yellow('Submitting coupled model'))
    keys = ['workdir', 'platform', 'walltime', 'datatable', 'diagtable',
            'fieldtable', 'executable', 'execdir', 'TRUNC', 'LEV', 'LV',
            'rootexp', 'mppnccombine']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts', environ)):
                output = run(fmt('. run_g4c_model.cray {mode} {start} '
                                 '{restart} {finish} {npes} {name}', environ))
    environ['JobID_model'] = re.search(".*JobIDmodel:\s*(.*)\s*",output).groups()[0]


@env_options
@task
def run_ocean_model(environ, **kwargs):
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
            'fieldtable', 'executable', 'mppnccombine']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{expdir}/runscripts', environ)):
                output = run(fmt('. run_g4c_model.cray {mode} {start} '
                                 '{restart} {finish} {npes} {name}', environ))
    environ['JobID_model'] = re.search(".*JobIDmodel:\s*(.*)\s*",output).groups()[0]


@env_options
@task
def run_model(environ, **kwargs):
    '''Run the model

    Used vars:
      expdir
      mode
      start
      restart
      finish
      name
    '''
    print(fc.yellow('Running model'))

    begin = datetime.strptime(str(environ['restart']), "%Y%m%d%H")
    end = datetime.strptime(str(environ['finish']), "%Y%m%d%H")
    if environ['restart_interval'] is None:
        delta = relativedelta(end, begin)
    else:
        interval, units = environ['restart_interval'].split()
        if not units.endswith('s'):
            units = units + 's'
        delta = relativedelta(**dict( [[units, int(interval)]] ))

    for period in genrange(begin, end, delta):
        finish = period + delta
        if finish > end:
            finish = end

        if environ['mode'] == 'cold':
            environ['restart'] = finish.strftime("%Y%m%d%H")
            environ['finish'] = environ['restart']
        else:
            environ['restart'] = period.strftime("%Y%m%d%H")
            environ['finish'] = finish.strftime("%Y%m%d%H")

        environ['days'] = (finish - period).days

        # TODO: set restart_interval in input.nml to be equal to delta

        if environ['type'] in ('atmos', 'coupled'):
            prepare_atmos_namelist(environ)
        if environ['type'] in ('mom4p1_falsecoupled', 'coupled'):
            prepare_ocean_namelist(environ)

        if environ['type'] == 'atmos':
            run_atmos_model(environ)
        elif environ['type'] == 'mom4p1_falsecoupled':
            run_ocean_model(environ)
        elif environ['type'] == 'coupled':
            run_coupled_model(environ)
        else:
            print(fc.red(fmt('Unrecognized type: {type}'), environ))
            sys.exit(1)

        if environ['type'] in ('mom4p1_falsecoupled', 'coupled'):
            run_pos_ocean(environ)
        if environ['type'] in ('atmos', 'coupled'):
            run_pos_atmos(environ)

        while check_status(environ, oneshot=True):
            time.sleep(GET_STATUS_SLEEP_TIME)

        prepare_restart(environ)
        environ['mode'] = 'warm'


@env_options
@task
def prepare_restart(environ, **kwargs):
    if environ['type'] == 'coupled':
        # copy ocean restarts for the day to ${workdir}/INPUT
        # for now running in same dir, so no need to copy atmos restarts (but it's a good thing to
        # do).

        #restart_init = environ['finish'][0:8]
        #files = run(fmt('ls {workdir}/RESTART/*' % restart_init, environ))
        #for rfile in files:
        #    run(fmt('cp {workdir}/RESTART/%s {workdir}/INPUT/%s' % (rfile, rfile.split('.')[2:]), environ))
            run(fmt('cp {workdir}/RESTART/* {workdir}/INPUT/', environ))


def _get_status(environ):
    with settings(warn_only=True):
        with hide('running', 'stdout'):
            job_ids = " ".join([environ[k] for k in environ.keys() if "JobID" in k])
            data = run(fmt("qstat -a %s" % job_ids, environ))
    statuses = {}
    if data.succeeded:
        header = None
        for line in data.split('\n'):
            if header:
                info = line.split()
                try:
                    statuses[info[0]] = dict(zip(header.split()[1:], info))
                except IndexError:
                    pass
            elif line.startswith('Job ID'):
                header = line
    return statuses


def _calc_ETA(rh, rm, percent):
    dt = rh*60 + rm
    m = dt / (percent or 1)
    remain = timedelta(minutes=m) - timedelta(minutes=dt)
    return remain.days / 24 + remain.seconds / 3600, (remain.seconds / 60) % 60


def _handle_mainjob(environ, status):
    logfile = fmt("{workdir}/logfile.000000.out", environ)
    fmsfile = fmt("{workdir}/fms.out", environ)
    if status['S'] == 'R':
        if not exists(logfile):
            print(fc.yellow('Preparing!'))
        else:
            try:
                if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
                    line = run('tac %s | grep -m1 yyyy' % fmsfile)
                    current = re.search('(\d{4})/(\s*\d{1,2})/(\s*\d{1,2})\s(\s*\d{1,2}):(\s*\d{1,2}):(\s*\d{1,2})', line)
                    try:
                        current = datetime(*[int(i) for i in current.groups()])
                    except AttributeError:
                        print(fc.yellow('Preparing!'))
                    else:
                        if environ['mode'] == 'warm':
                            start = environ['restart']
                        else:
                            start = environ['start']
                        begin = datetime.strptime(str(start), "%Y%m%d%H")
                        end = datetime.strptime(
                            str(environ['finish']), "%Y%m%d%H") + relativedelta(days=+1)
                        count = current - begin
                        total = end - begin
                        percent = float(total_seconds(count)) / total_seconds(total)

                        rh, rm = map(float, status['Time'].split(':'))
                        remh, remm = _calc_ETA(rh, rm, percent)
                        print(fc.yellow('Model running time: %s, %.2f %% completed, Estimated %02d:%02d'
                              % (status['Time'], 100*percent, remh, remm)))
                else: # TODO: how to calculate that for atmos?
                    pass
            except: # ignore all errors in this part
                pass
            else:
                print(fc.yellow('Model: %s' % JOB_STATES[status['S']]))
    else:
        print(fc.yellow('Model: %s' % JOB_STATES[status['S']]))


@env_options
@task
def check_status(environ, **kwargs):
    print(fc.yellow('Checking status'))
    statuses = _get_status(environ)
    if statuses:
        while statuses:
            for status in sorted(statuses.values(), key=lambda x: x['ID']):
                if status['ID'] in environ.get('JobID_model', ""):
                    _handle_mainjob(environ, status)
                elif status['ID'] in environ.get('JobID_pos_ocean', ""):
                    print(fc.yellow('Ocean post-processing: %s' % JOB_STATES[status['S']]))
                elif status['ID'] in environ.get('JobID_pos_atmos', ""):
                    print(fc.yellow('Atmos post-processing: %s' % JOB_STATES[status['S']]))
            if kwargs.get('oneshot', False) == False:
                time.sleep(GET_STATUS_SLEEP_TIME)
                statuses = _get_status(environ)
            else:
                statuses = {}
            print()
        return 1
    else:
        print(fc.yellow('No jobs running.'))
        return 0


@env_options
@task
def kill_experiment(environ, **kwargs):
    print(fc.yellow('Killing experiment'))
    statuses = _get_status(environ)
    if statuses:
        for status in statuses.values():
            s = status['Jobname']
            if (s in fmt('M_{name}', environ) or
                s in fmt('C_{name}', environ) or
                s in fmt('P_{name}', environ)):
                run('qdel %s' % status['ID'].split('-')[0])


@env_options
@task
def prepare_expdir(environ, **kwargs):
    '''Prepare experiment dir on remote side.

    Used vars:
      expdir
      execdir
      comb_exe
      PATH2
    '''
    print(fc.yellow('Preparing expdir'))
    run(fmt('mkdir -p {expdir}', environ))
    run(fmt('mkdir -p {execdir}', environ))
    if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
        run(fmt('mkdir -p {comb_exe}', environ))
    if environ['type'] in 'atmos':
        run(fmt('mkdir -p {PATH2}', environ))
    run(fmt('cp -R {expfiles}/exp/{name}/* {expdir}', environ))


@env_options
@task
def clean_experiment(environ, **kwargs):
    run(fmt('rm -rf {expdir}', environ))
    run(fmt('rm -rf {rootexp}', environ))
    run(fmt('rm -rf {execdir}', environ))
    if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
        run(fmt('rm -rf {comb_exe}', environ))
    if environ['type'] in 'atmos':
        run(fmt('rm -rf {PATH2}', environ))
    run(fmt('rm -rf {workdir}', environ))


@env_options
@task
def prepare_workdir(environ, **kwargs):
    '''Prepare output dir

    Used vars:
      workdir
      workdir_template
    '''
    print(fc.yellow('Preparing workdir'))
    run(fmt('mkdir -p {workdir}', environ))
    run(fmt('cp -R {workdir_template}/* {workdir}', environ))
    run(fmt('touch {workdir}/time_stamp.restart', environ))
    # TODO: lots of things.
    #  1) generate atmos inputs (gdas_to_atmos from oper scripts)
    #  2) copy restart files from somewhere (emanuel's spinup, for example)


@env_options
@task
def clean_model_compilation(environ, **kwargs):
    '''Clean compiled files

    Used vars:
      execdir
      envconf
      makeconf
    '''
    print(fc.yellow("Cleaning code dir"))
    with shell_env(environ, keys=['root', 'expdir', 'comp']):
        with cd(fmt('{execdir}', environ)):
            with prefix(fmt('source {envconf}', environ)):
                run(fmt('make -f {makeconf} clean', environ))


@env_options
@task
def compile_atmos_pre(environ, **kwargs):
    with shell_env(environ, keys=['PATH2']):
        with prefix(fmt('source {envconf_pos}', environ)):
            with cd(environ['preatmos_src']):
                fix_atmos_makefile(environ)
                run(fmt('make cray', environ))


@env_options
@task
def compile_atmos_pos(environ, **kwargs):
    with shell_env(environ, keys=['PATH2']):
        with prefix(fmt('source {envconf_pos}', environ)):
            with cd(environ['posgrib_src']):
                fix_atmos_makefile(environ)
                run(fmt('make cray', environ))


@env_options
@task
def compile_ocean_pos(environ, **kwargs):
    with shell_env(environ, keys=['root', 'platform']):
        with prefix(fmt('source {envconf}', environ)):
            with cd(environ['comb_exe']):
                run(fmt('make -f {comb_src}/Make_combine', environ))


@env_options
@task
def compile_ocean_model(environ, **kwargs):
    keys = ['comp', 'code_dir', 'root', 'type', 'mkmf_template', 'executable']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                run(fmt('/usr/bin/tcsh {ocean_makeconf}', environ))


@env_options
@task
def compile_atmos_model(environ, **kwargs):
    with shell_env(environ, keys=['root', 'executable']):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                run(fmt('make -f {atmos_makeconf}', environ))


@env_options
@task
def compile_coupled_model(environ, **kwargs):
    keys = ['root', 'expdir', 'comp']
    with shell_env(environ, keys=keys):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                #TODO: generate RUNTM and substitute
                run(fmt('make -f {makeconf}', environ))


@env_options
@task
def compile_model(environ, **kwargs):
    '''Compile model and post-processing tools.

    Used vars:
      envconf
      execdir
      makeconf
      comb_exe
      comb_src
      envconf_pos
      posgrib_src
      PATH2
    '''
    print(fc.yellow("Compiling code"))
    if environ['type'] == 'mom4p1_falsecoupled':
        compile_ocean_model(environ)
        compile_ocean_pos(environ)
    elif environ['type'] == 'atmos':
        compile_atmos_model(environ)
        compile_atmos_pre(environ)
        compile_atmos_pos(environ)
    elif environ['type'] == 'coupled':
        compile_coupled_model(environ)
        compile_ocean_pos(environ)
        compile_atmos_pre(environ)
        compile_atmos_pos(environ)


@env_options
@task
def fix_atmos_makefile(environ, **kwargs):
    '''Stupid pre and pos makefiles...'''
    run("sed -i.bak -r -e 's/^PATH2/#PATH2/g' Makefile")


@env_options
@task
def check_code(environ, **kwargs):
    ''' Checks out code in the remote side.

    Used vars:
      clean_checkout
      code_dir
      code_repo
      code_dir
      code_branch
      revision
      executable
    '''
    print(fc.yellow("Checking code"))

    changed = False
    if environ['clean_checkout']:
        run(fmt('rm -rf {code_dir}', environ))
        changed = True

    if not exists(environ['code_dir']):
        print(fc.yellow("Creating new repository"))
        run(fmt('mkdir -p {code_dir}', environ))
        run(fmt('hg clone {code_repo} {code_dir}', environ))
        changed = True

    with cd(environ['code_dir']):
        print(fc.yellow("Updating existing repository"))

        # First check if there is any change in repository, or
        # if requesting a different branch/revision
        with settings(warn_only=True):
            res = run('hg incoming')
        if res.return_code == 0: # New changes!
            run('hg pull')
            changed = True
        rev = environ.get('revision', None)
        curr_rev = run('hg id -i').strip('+')
        run(fmt('hg update {code_branch}', environ))
        if rev and rev != 'last' and rev != curr_rev:
            run(fmt('hg update -r{revision}', environ))
            changed = True

        # Need to check if executables exists!
        if not exists(environ['executable']):
            changed = True

    return changed

@env_options
@task
def link_agcm_inputs(environ, **kwargs):
    '''Copy AGCM inputs for model run and post-processing to the right place

    Used vars:
      rootexp
      agcm_pos_inputs
      agcm_model_inputs
    '''
    for d in ['model', 'pos']:
        run(fmt('mkdir -p {rootexp}/AGCM-1.0/%s' % d, environ))
        if not exists(fmt('{rootexp}/AGCM-1.0/%s/datain' % d, environ)):
            print(fc.yellow(fmt("Linking AGCM %s input data" % d, environ)))
            if not exists(fmt('{agcm_%s_inputs}' % d, environ)):
                run(fmt('mkdir -p {agcm_%s_inputs}' % d, environ))
                run(fmt('cp -R $ARCHIVE_OCEAN/database/AGCM-1.0/%s/datain '
                        '{agcm_%s_inputs}' % (d, d), environ))
            run(fmt('cp -R {agcm_%s_inputs} '
                    '{rootexp}/AGCM-1.0/%s/datain' % (d, d), environ))
