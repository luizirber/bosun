 #!/usr/bin/env python

from __future__ import with_statement
from __future__ import print_function
import time
from datetime import datetime

from fabric.api import run, cd, prefix, settings, hide
import fabric.colors as fc
from fabric.contrib.files import exists
from fabric.decorators import task
from dateutil.relativedelta import relativedelta

from bosun.environ import env_options, fmt
from bosun.utils import genrange


__all__ = ['check_code', 'check_status', 'clean_experiment', 'compile_model',
           'instrument_code', 'kill_experiment', 'run_model', 'archive_model']


@task
@env_options
def instrument_code(environ, **kwargs):
    '''Instrument code using Cray's perftools.

    Used vars:
      executable
      expdir

    Depends on:
      compile_model
    '''
    print(fc.yellow('Rebuilding executable with instrumentation'))
    with prefix('module load perftools'):
        compile_model(environ)
        if exists(fmt('{executable}+apa', environ)):
            run(fmt('rm {executable}+apa', environ))
        run(fmt('pat_build -O {expdir}/instrument_coupler.apa '
                '-o {executable}+apa {executable}', environ))
        environ['executable'] = fmt('{executable}+apa', environ)


@task
@env_options
def run_model(environ, **kwargs):
    '''Run the model

    Used vars:
      expdir
      mode
      start
      restart
      restart_interval
      finish
      name
      days
      type

    Depends on:
      agcm.prepare_namelist
      mom4.prepare_namelist
      agcm.run_model
      mom4.run_model
      coupled.run_model
      agcm.run_post
      mom4.run_post
      check_status
      prepare_restart
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
        delta = relativedelta(**dict([[units, int(interval)]]))

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

        environ['model'].check_restart(environ)

        # TODO: set months and days? only use days?
        if period.day == finish.day:
            environ['months'] = (12 * (finish.year - period.year) +
                                 finish.month - period.month)
        else:
            environ['days'] = (finish - period).days

        # TODO: set restart_interval in input.nml to be equal to delta

        environ['model'].prepare_namelist(environ)
        environ['model'].run_model(environ)
        environ['model'].run_post(environ)

        while check_status(environ, oneshot=True):
            time.sleep(environ.get('status_sleep_time', 60))

        environ['model'].verify_run(environ)
        environ['model'].archive(environ)
        environ['mode'] = 'warm'


def _get_status(environ):
    ''' Retrieve PBS status from remote side '''
    with settings(warn_only=True):
        with hide('running', 'stdout'):
            job_ids = " ".join([environ[k] for k in environ.keys()
                                if "JobID" in k])
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


@task
@env_options
def check_status(environ, **kwargs):
    print(fc.yellow('Checking status'))

    statuses = _get_status(environ)
    if not statuses:
        print(fc.yellow('No jobs running.'))
        return False

    while statuses:
        for status in statuses.values():
            environ['model'].check_status(environ, status)

        if kwargs.get('oneshot', False):
            statuses = {}
        else:
            time.sleep(environ.get('status_sleep_time', 60))
            statuses = _get_status(environ)
        print()

    return True


@task
@env_options
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


@task
@env_options
def clean_experiment(environ, **kwargs):
    run(fmt('rm -rf {expdir}', environ))
    run(fmt('rm -rf {rootexp}', environ))
    run(fmt('rm -rf {execdir}', environ))
    run(fmt('rm -rf {workdir}', environ))
    environ['model'].clean_experiment(environ)


@task
@env_options
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
    environ['model'].compile_model(environ)
    environ['model'].compile_pre(environ)
    environ['model'].compile_post(environ)


@task
@env_options
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
            res = run(fmt('hg incoming -b {code_branch}', environ))
        if res.return_code == 0:  # New changes!
            run('hg pull')
            changed = True
        rev = environ.get('revision', None)
        curr_rev = run('hg id -i').strip('+')
        run(fmt('hg update {code_branch} --clean', environ))
        if rev and rev != 'last' and rev != curr_rev:
            run(fmt('hg update -r{revision}', environ))
            changed = True

        # Need to check if executables exists!
        if not exists(environ['executable']):
            changed = True

    return changed


@task
@env_options
def archive_model(environ, **kwargs):
    environ['model'].archive(environ)


@task
@env_options
def prepare_expdir(environ, **kwargs):
    environ['model'].prepare_expdir(environ)
