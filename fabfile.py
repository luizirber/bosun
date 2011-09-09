#!/usr/bin/env python

from __future__ import with_statement
import re
import os.path

from fabric.api import run, local, cd, settings, show, prefix
import fabric.colors as fc
from fabric.contrib.files import exists, comment
from fabric.decorators import task
import yaml


class NoEnvironmentSetException(Exception):
    pass


def env_options(f):
    def _wrapped_env(*args, **kw):
        try:
            environ = kw['environ']
            del kw['environ']
        except KeyError:
            expdir = kw.get('expdir', 'exp')
            filename = kw.get('filename', 'namelist.yaml')
            environ = _read_config(os.path.join(expdir, filename))

        if environ is None:
            raise NoEnvironmentSetException
        else:
            return f(environ=environ, **kw)

    return _wrapped_env

def shell_env(args):
    env_vars = " ".join(["=".join((key, str(value))) for (key, value) in args.items()])
    return prefix("export %s" % env_vars)

@env_options
@task
def deploy(environ=None, instrument=False, **kwargs):
    print(fc.green("Started"))
    check_code(environ)
    if instrument:
        with prefix('module load perftools'):
            clean_model_compilation(environ)
            compile_model(environ)
            instrument_code(environ)
    else:
        compile_model(environ)
    prepare_expdir(environ)
    link_agcm_inputs(environ)
    prepare_workdir(environ)
    fix_submit(environ)
    run_model(environ)


def _read_config(filename):

    def rec_replace(env, key, value):
        finder = re.compile('\$\{(\w*)\}')
        ret_value = value

        try:
            keys = finder.findall(value)
        except TypeError:
            return value

        for k in keys:
            if k in env:
                ret_value = rec_replace(env, k, ret_value.replace('${%s}' % k, env[k]))

        return ret_value


    def env_replace(old_env):
        new_env = {}
        for k in old_env.keys():
            try:
                old_env[k].split(' ')
            except AttributeError:
                new_env[k] = old_env[k]
            else:
                new_env[k] = rec_replace(old_env, k, old_env[k])
        return new_env

    data = open(filename, 'r').read()
    TOKEN = re.compile(r'''\$\{(.*?)\}''')

    d = yaml.load(data)
    #while set(TOKEN.findall(data)) & set(d.keys()):
    #    d = yaml.load(data)
    #    data = TOKEN.sub(lambda m: d.get(m.group(1), '${%s}' % m.group(1)), data)
    new_d = env_replace(d)

    # FIXME: sanitize input!
    return new_d


@env_options
@task
def instrument_code(environ=None, **kwargs):
    print(fc.yellow('Rebuilding executable with instrumentation'))
    run('cp ~/pat/instrument_coupler.apa {expdir}/exec/'.format(**environ))
    run('pat_build -O {expdir}/exec/instrument_coupler.apa -o {executable}+apa {executable}'.format(**environ))
    env['executable'] =  ''.join((environ['executable'], '+apa'))
#    run("sed -i.bak -r -e 's/^export executable=.*$/export executable=\$\{expdir\}\/exec\/fms_mom4p1_coupled.x+apa/g' NAMELIST.environment")


@env_options
@task
def run_model(environ=None, **kwargs):
    print(fc.yellow('Running model'))
    with shell_env(environ):
        with cd('{root}/MOM4p1/exp/cpld2.1'.format(**environ)):
            run('. run_g4c_model.cray cold 2007010100 2007011000 2007011000 48 rodteste')


@env_options
@task
def prepare_expdir(environ=None, **kwargs):
    print(fc.yellow('Preparing expdir'))
    with cd(environ['expdir']):
        run('cp -R {root}/MOM4p1/exp/cpld2.1/tables {expdir}'.format(**environ))
        run('touch {expdir}/tables/data_override'.format(**environ))


@env_options
@task
def prepare_workdir(environ=None, **kwargs):
    print(fc.yellow('Preparing workdir'))
    run('mkdir -p {workdir}'.format(**environ))
    run('cp -R $ARCHIVE_OCEAN/database/work20070101/* {workdir}'.format(**environ))
    run('touch {workdir}/time_stamp.restart'.format(**environ))


@env_options
@task
def clean_model_compilation(environ=None, **kwargs):
    print(fc.yellow("Cleaning code dir"))
    with shell_env(environ):
        with cd(environ['expdir']):
            with cd('exec'):
                with prefix('source {root}/MOM4p1/bin/environs.{comp}'.format(**environ)):
                    run('make -f {root}/CPLD2.1/source/Make_cpldp1.pgi clean'.format(**environ))


@env_options
@task
def compile_model(environ=None, **kwargs):
    print(fc.yellow("Compiling code"))
    run('mkdir -p {expdir}/exec'.format(**environ))
    with shell_env(environ):
        with cd(environ['expdir']):
            with cd('exec'):
                #TODO: generate RUNTM and substitute
                with prefix('source {root}/MOM4p1/bin/environs.{comp}'.format(**environ)):
                    run('make -f {root}/CPLD2.1/source/Make_cpldp1.pgi'.format(**environ))


@env_options
@task
def check_code(environ=None, **kwargs):
    print(fc.yellow("Checking code"))
    if environ['clean_checkout']:
        run('rm -rf {code_dir}'.format(**environ))
    if not exists(environ['code_dir']):
        print(fc.yellow("Creating new repository"))
        run('hg clone {code_repo} {code_dir}'.format(**environ))
    with cd(environ['code_dir']):
        print(fc.yellow("Updating existing repository"))
        run('hg pull')
        run('hg update {code_branch}'.format(**environ))
#        run('hg update -r{revision}'.format(**env))


@env_options
@task
def link_agcm_inputs(environ=None, **kwargs):
    run('mkdir -p {rootexp}/AGCM-1.0/model'.format(**environ))
    if not exists('{rootexp}/AGCM-1.0/model/datain'.format(**environ)):
        print(fc.yellow("Linking AGCM input data"))
        run('ln -s $SUBMIT_OCEAN/database/AGCM-1.0/model/datain {rootexp}/AGCM-1.0/model/datain'.format(**environ))


@env_options
@task
def fix_submit(environ=None, **kwargs):
    with cd('{root}/MOM4p1/exp/cpld2.1'.format(**environ)):
        run("sed -i.bak -r -e 's/aprun -n \$\{npes\} submit/aprun -n \$\{npes\} .\/submit/g' run_g4c_model.cray")
        run("sed -i.bak -r -e 's/\. \.\/NAMELIST/\#\. \.\/NAMELIST/g' run_g4c_model.cray")
