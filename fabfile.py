#!/usr/bin/env python

from __future__ import with_statement

from fabric.api import run, local, cd, settings, show, env
from fabric.colors import green, red, yellow
from fabric.contrib.files import exists, comment
from fabric.decorators import hosts


@hosts('tupa')
def deploy():
    print(green("Started"))
    submit_dir = '$SUBMIT_HOME'
    code_dir = 'run'
    with cd(submit_dir):
        check_code(code_dir, 'monin')
        with cd(code_dir):
            with cd('Modelos/AGCM-1.0/model'):
                link_agcm_inputs()
            with cd('Modelos/MOM4p1/exp/cpld2.1'):
                replace_vars()
                compile_model()
                prepare_workdir()
                run_model()


def run_model():
    run('./run_g4c_model.cray cold 2007010100 2007020100 2007020100 48 rodteste')


def prepare_workdir():
    run('mkdir -p $WORK_HOME/Modelos/MOM4p1/cpld2.1/')
    run('cp -R $ARCHIVE_OCEAN/database/work20070101/ $WORK_HOME/Modelos/MOM4p1/cpld2.1/')
    run('touch $WORK_HOME/Modelos/MOM4p1/cpld2.1/work20070101/time_stamp.restart')


def compile_model():
    print(yellow("Compiling code"))
    if not exists('exec'):
        run('mkdir exec')
    run('./run_g4c_model.cray make')


def check_code(code_dir, branch):
    # FIXME: sanitize input!
    print(yellow("Checking code"))
    if not exists(code_dir):
        print(yellow("Creating new repository"))
        run('hg clone $ARCHIVE_OCEAN/repos %s' %  code_dir)
    with cd(code_dir):
        print(yellow("Updating existing repository"))
        run('hg update %s' % branch)


def link_agcm_inputs():
    if not exists('datain'):
        print(yellow("Linking AGCM input data"))
        run('ln -s $SUBMIT_OCEAN/database/AGCM-1.0/model/datain datain')


def replace_vars():
    # TODO: modificar NAMELIST.environment
    # TODO: usar upload_template em vez de sed?
    print(yellow("Replacing variables in NAMELIST.environment"))

    print(yellow("Replacing variable root"))
    run("sed -i.bak -r -e 's/^export root=.*$/export root=\$\{SUBMIT_HOME\}\/run\/Modelos/g' NAMELIST.environment")

    print(yellow("Replacing variable rootexp"))
    run("sed -i.bak -r -e 's/^export rootexp=.*$/export rootexp=\$\{SUBMIT_HOME\}\/run\/Modelos/g' NAMELIST.environment")

    print(yellow("Replacing variable workdir"))
    run("sed -i.bak -r -e 's/^export workdir=.*$/export workdir=\$\{WORK_HOME\}\/Modelos\/MOM4p1\/\$\{name\}\/work20070101/g' NAMELIST.environment")

    print(yellow("Replacing variable diagtable"))
    run("sed -i.bak -r -e 's/^export diagtable=.*$/export diagtable=\$\{expdir\}\/tables\/diagtable/g' NAMELIST.environment")

    print(yellow("Replacing variable fieldtable"))
    run("sed -i.bak -r -e 's/^export fieldtable=.*$/export fieldtable=\$\{expdir\}\/tables\/fieldtable/g' NAMELIST.environment")

    print(yellow("Replacing variable datatable"))
    run("sed -i.bak -r -e 's/^export datatable=.*$/export datatable=\$\{expdir\}\/tables\/datatable/g' NAMELIST.environment")

    run('touch tables/data_override')
