#!/usr/bin/env python

from __future__ import with_statement
import re

from fabric.api import run, local, cd, settings, show, prefix
import fabric.colors as fc
from fabric.contrib.files import exists, comment
from fabric.decorators import hosts
import yaml


def shell_env(args):
    env_vars = " ".join(["=".join((key, str(value))) for (key, value) in args.items()])
    return prefix("export %s" % env_vars)


@hosts('tupa')
def deploy(environ=None, filename='namelist.yaml', instrument=False):
    print(fc.green("Started"))
    if not environ:
        environ = _read_config('namelist.yaml')
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


def instrument_code(environ=None):
    print(fc.yellow('Rebuilding executable with instrumentation'))
    if not environ:
        environ = _read_config('namelist.yaml')
    run('cp ~/pat/instrument_coupler.apa {expdir}/exec/'.format(**environ))
    run('pat_build -O {expdir}/exec/instrument_coupler.apa -o {executable}+apa {executable}'.format(**environ))
    env['executable'] =  ''.join((environ['executable'], '+apa'))
#    run("sed -i.bak -r -e 's/^export executable=.*$/export executable=\$\{expdir\}\/exec\/fms_mom4p1_coupled.x+apa/g' NAMELIST.environment")


def run_model(environ=None):
    print(fc.yellow('Running model'))
    if not environ:
        environ = _read_config('namelist.yaml')
    with shell_env(environ):
        with cd('{root}/MOM4p1/exp/cpld2.1'.format(**environ)):
            run('. run_g4c_model.cray cold 2007010100 2007011000 2007011000 48 rodteste')


def prepare_expdir(environ=None):
    print(fc.yellow('Preparing expdir'))
    if not environ:
        environ = _read_config('namelist.yaml')
    with cd(environ['expdir']):
        run('cp -R {root}/MOM4p1/exp/cpld2.1/tables {expdir}'.format(**environ))
        run('touch {expdir}/tables/data_override'.format(**environ))


def prepare_workdir(environ=None):
    print(fc.yellow('Preparing workdir'))
    if not environ:
        environ = _read_config('namelist.yaml')
    run('mkdir -p {workdir}'.format(**environ))
    run('cp -R $ARCHIVE_OCEAN/database/work20070101/* {workdir}'.format(**environ))
    run('touch {workdir}/time_stamp.restart'.format(**environ))


def clean_model_compilation(environ=None):
    print(fc.yellow("Cleaning code dir"))
    if not environ:
        environ = _read_config('namelist.yaml')
    with shell_env(environ):
        with cd(environ['expdir']):
            with cd('exec'):
                with prefix('source {root}/MOM4p1/bin/environs.{comp}'.format(**environ)):
                    run('make -f {root}/CPLD2.1/source/Make_cpldp1.pgi clean'.format(**environ))


def compile_model(environ=None):
    print(fc.yellow("Compiling code"))
    if not environ:
        environ = _read_config('namelist.yaml')
    run('mkdir -p {expdir}/exec'.format(**environ))
    with shell_env(environ):
        with cd(environ['expdir']):
            with cd('exec'):
                #TODO: generate RUNTM and substitute
                with prefix('source {root}/MOM4p1/bin/environs.{comp}'.format(**environ)):
                    run('make -f {root}/CPLD2.1/source/Make_cpldp1.pgi'.format(**environ))


def check_code(environ=None):
    print(fc.yellow("Checking code"))
    if not environ:
        environ = _read_config('namelist.yaml')
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


def link_agcm_inputs(environ=None):
    if not environ:
        environ = _read_config('namelist.yaml')
    run('mkdir -p {rootexp}/AGCM-1.0/model'.format(**environ))
    if not exists('{rootexp}/AGCM-1.0/model/datain'.format(**environ)):
        print(fc.yellow("Linking AGCM input data"))
        run('ln -s $SUBMIT_OCEAN/database/AGCM-1.0/model/datain {rootexp}/AGCM-1.0/model/datain'.format(**environ))


def fix_submit(environ=None):
    if not environ:
        environ = _read_config('namelist.yaml')
    with cd('{root}/MOM4p1/exp/cpld2.1'.format(**environ)):
        run("sed -i.bak -r -e 's/aprun -n \$\{npes\} submit/aprun -n \$\{npes\} .\/submit/g' run_g4c_model.cray")
        run("sed -i.bak -r -e 's/\. \.\/NAMELIST/\#\. \.\/NAMELIST/g' run_g4c_model.cray")
