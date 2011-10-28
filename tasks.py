#!/usr/bin/env python

from __future__ import with_statement
from __future__ import print_function
import re
import os.path
import functools
from pipes import quote
from pprint import pprint
import time
from datetime import timedelta

from fabric.api import run, cd, prefix, put, get, lcd, local, settings, hide
import fabric.colors as fc
from fabric.contrib.files import exists
from fabric.decorators import task
import yaml


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


__all__ = ['prepare_expdir', 'check_code', 'instrument_code', 'compile_model',
           'link_agcm_inputs', 'prepare_workdir', 'run_model', 'env_options',
           'check_status']


class NoEnvironmentSetException(Exception):
    pass


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
                       'name': name}
            with hide('running', 'stdout', 'stderr', 'warnings'):
                local('mkdir -p workspace')
                with lcd('workspace'):
                    if not exists(fmt('autobot_exps', environ)):
                        run(fmt('hg clone {exp_repo} autobot_exps', environ))
                    else:
                        with cd(fmt('autobot_exps', environ)):
                            run('hg pull')
                            run('hg update')

                    get(fmt('autobot_exps/exp/{name}/namelist.yaml', environ),
                        'exp.yaml')

            environ = _read_config('workspace/exp.yaml')
            environ = _expand_config_vars(environ, updates=kw)

        if environ is None:
            raise NoEnvironmentSetException
        else:
            return f(environ, **kw)

    return functools.update_wrapper(_wrapped_env, f)


def shell_env(args):
    '''Context manager that can send shell env variables to remote side.

    Really dumb context manager for Fabric, in fact. It just generates a new
    prefix with an export before the actual command. Something like

    $ export workdir=${HOME}/teste expdir=${HOME}/teste/exp <cmd>
    '''
    env_vars = " ".join(["=".join((key, str(value)))
                                   for (key, value) in args.items()])
    return prefix("export %s" % env_vars)


def fmt(s, e):
    '''String formatting and sanitization function'''
    return s.format(**e)


def _expand_config_vars(d, updates=None):

    def rec_replace(env, value):
        finder = re.compile('\$\{(\w*)\}')
        ret_value = value

        try:
            keys = finder.findall(value)
        except TypeError:
            return value

        for k in keys:
            if k in env:
                ret_value = rec_replace(env,
                                        ret_value.replace('${%s}' % k, env[k]))

        return ret_value

    def env_replace(old_env):
        new_env = {}
        for k in old_env.keys():
            try:
                # tests if value is a string
                old_env[k].split(' ')
            except AttributeError:
                # if it's not, just set new_env with it
                new_env[k] = old_env[k]
            else:
                # if it is, start replacing vars
                new_env[k] = rec_replace(old_env, old_env[k])
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
        clean_model_compilation(environ)
        compile_model(environ)
        if exists(fmt('{executable}+apa', environ)):
            run(fmt('rm {executable}+apa', environ))
        run(fmt('pat_build -O {expdir}/instrument_coupler.apa '
                '-o {executable}+apa {executable}', environ))
        environ['executable'] = fmt('{executable}+apa', environ)


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
    with shell_env(environ):
        with cd(fmt('{expdir}/runscripts', environ)):
            run(fmt('. run_g4c_model.cray {mode} {start} {restart} '
                    '{finish} 48 {name}', environ))


def _update_status(header):
    with settings(warn_only=True):
        data = run("qstat -u $USER @sdb @aux20-eth4|grep -e .sdb -e .aux20")
    statuses = {}
    if data.succeeded:
        for line in data.split('\n'):
            info = line.split()
            statuses[info[0]] = dict(zip(header.split()[1:], info))
    return statuses


def _calc_ETA(rh, rm, percent):
    dt = rh*60 + rm
    m = dt / percent
    remain = timedelta(minutes=m) - timedelta(minutes=dt)
    return remain.days / 24 + remain.seconds / 3600, (remain.seconds / 60) % 60


def _handle_mainjob(status):
    # MORE BLACK MAGIC THAN DUMBLEDORE CAN HANDLE! Thanks Mano =]
    root = run(r"qstat -f %s|sed -n '/Submit_arguments/,//p'|sed ':a;$!N;s/\n//g;ta'|sed 's/\t//g'" % status['ID'])
    logfile = "%s/logfile.000000.out" % os.path.dirname(root.split('= ')[-1])
    if status['S'] == 'R':
        if not exists(logfile):
            print(fc.yellow('Preparing!'))
        else:
            line = run('grep cpld %s | tail -1' % logfile)
            try:
                count, total = map(float, line.split()[2:])
            except ValueError:
                print(fc.yellow('Preparing!'))
            else:
                percent = count/total
                rh, rm = map(float, status['Time'].split(':'))
                remh, remm = _calc_ETA(rh, rm, percent)
                print(fc.yellow('Model running time: %s, %.2f %% completed, Remaining %02d:%02d'
                      % (status['Time'], 100*percent, remh, remm)))
    else:
        print(fc.yellow(JOB_STATES[status['S']]))


@env_options
@task
def check_status(environ, **kwargs):
    print(fc.yellow('Checking status'))
    with settings(warn_only=True):
        header = run(fmt("qstat -u $USER|grep Username", environ))
    if header.succeeded:
        statuses = _update_status(header)
        while statuses:
            for status in sorted(statuses.values(), key=lambda x: x['ID']):
                if status['Jobname'] in fmt('M_{name}', environ):
                    _handle_mainjob(status)
                elif status['Jobname'] in fmt('C_{name}', environ):
                    print(fc.yellow('Ocean post-processing: %s' % JOB_STATES[status['S']]))
                elif status['Jobname'] in fmt('P_{name}', environ):
                    print(fc.yellow('Atmos post-processing: %s' % JOB_STATES[status['S']]))
            time.sleep(10)
            statuses = _update_status(header)
            print()
    else:
        print(fc.yellow('No jobs running.'))


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
    run(fmt('mkdir -p {comb_exe}', environ))
    run(fmt('mkdir -p {PATH2}', environ))
    run(fmt('cp -R {name}/workspace/exp/{name}/* {expdir}', environ))


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
    with shell_env(environ):
        with cd(fmt('{execdir}', environ)):
            with prefix(fmt('source {envconf}', environ)):
                run(fmt('make -f {makeconf} clean', environ))


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
    with shell_env(environ):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                #TODO: generate RUNTM and substitute
                run(fmt('make -f {makeconf}', environ))
            with cd(environ['comb_exe']):
                run(fmt('make -f {comb_src}/Make_combine', environ))
        with prefix(fmt('source {envconf_pos}', environ)):
            with cd(environ['posgrib_src']):
                fix_posgrib_makefile(environ)
                run(fmt('mkdir -p {PATH2}', environ))
                run(fmt('make cray', environ))


@env_options
@task
def fix_posgrib_makefile(environ, **kwargs):
    '''Stupid posgrib makefile...'''
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
    '''
    print(fc.yellow("Checking code"))
    if environ['clean_checkout']:
        run(fmt('rm -rf {code_dir}', environ))
    if not exists(environ['code_dir']):
        print(fc.yellow("Creating new repository"))
        run(fmt('hg clone {code_repo} {code_dir}', environ))
    with cd(environ['code_dir']):
        print(fc.yellow("Updating existing repository"))
        run('hg pull')
        #run(fmt('hg update {code_branch}', environ))
        run(fmt('hg update -r{revision}', environ))


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
