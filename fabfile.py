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
    print env_vars
    return prefix("export %s" % env_vars)


@hosts('tupa')
def deploy(filename='namelist.yaml', instrument=False):
    print(fc.green("Started"))
    environ = _read_config(filename)
    with shell_env(environ):
        check_code(environ)
        if instrument:
            with prefix('module load perftools'):
#            clean_model_compilation()
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
    data = open(filename, 'r').read()
    TOKEN = re.compile(r'''\$\{(.*?)\}''')

    d = yaml.load(data)
    while set(TOKEN.findall(data)) & set(d.keys()):
        d = yaml.load(data)
        data = TOKEN.sub(lambda m: d.get(m.group(1), '${%s}' % m.group(1)), data)

    return d


def instrument_code(env):
    print(fc.yellow('Rebuilding executable with instrumentation'))
    run('cp ~/pat/instrument_coupler.apa ${expdir}/exec/')
    run('pat_build -O ${expdir}/exec/instrument_coupler.apa -o ${executable}+apa ${executable}')
    run("sed -i.bak -r -e 's/^export executable=.*$/export executable=\$\{expdir\}\/exec\/fms_mom4p1_coupled.x+apa/g' NAMELIST.environment")


def run_model(env):
    print(fc.yellow('Running model'))
    run('. run_g4c_model.cray cold 2007010100 2007011000 2007011000 48 rodteste')


def prepare_expdir(env):
    print(fc.yellow('Preparing expdir'))
    run('cp -R tables ${expdir}')
    run('touch {0[expdir]}/tables/data_override'.format(env))


def prepare_workdir(env):
    print(fc.yellow('Preparing workdir'))
    run('mkdir -p ${workdir}')
    run('cp -R $ARCHIVE_OCEAN/database/work20070101/* ${workdir}')
    run('touch ${workdir}/time_stamp.restart')


def clean_model_compilation(env):
    print(fc.yellow("Cleaning code dir"))
    with cd(env['expdir']):
        with cd('exec'):
            run('make -f {root}/CPLD2.1/source/Make_cpldp1 clean'.format(**env))


def compile_model(env):
    print(fc.yellow("Compiling code"))
    run('mkdir -p {expdir}/exec'.format(**env))
    with cd(env['expdir']):
        with cd('exec'):
            #TODO: generate RUNTM and substitute
            with prefix('source {root}/MOM4p1/bin/environs.{comp}'.format(**env)):
                run('make -f {root}/CPLD2.1/source/Make_cpldp1.pgi'.format(**env))


def check_code(env):
    # FIXME: sanitize input!
    print(fc.yellow("Checking code"))
    if env['clean_checkout']:
        run('rm -rf %s' % env['code_dir'])
    if not exists(env['code_dir']):
        print(fc.yellow("Creating new repository"))
        run('hg clone %s %s' % (env['code_repo'], env['code_dir']))
    with cd(env['code_dir']):
        print(fc.yellow("Updating existing repository"))
        run('hg pull')
        run('hg update {code_branch}'.format(**env))
#        run('hg update -r{revision}'.format(**env))


def link_agcm_inputs(env):
    run('mkdir -p {0[rootexp]}/AGCM-1.0/model'.format(env))
    if not exists('${rootexp}/AGCM-1.0/model/datain'):
        print(fc.yellow("Linking AGCM input data"))
        run('ln -s $SUBMIT_OCEAN/database/AGCM-1.0/model/datain ${rootexp}/AGCM-1.0/model/datain')


def fix_submit():
    run("sed -i.bak -r -e 's/^aprun -n \$\{npes\} submit/aprun -n \$\{npes\} .\/submit/g' run_g4c_model.cray")
