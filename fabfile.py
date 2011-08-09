#!/usr/bin/env python

from __future__ import with_statement

from fabric.api import run, local, cd, settings, show, env, prefix
from fabric.colors import green, red, yellow
from fabric.contrib.files import exists, comment
from fabric.decorators import hosts



@hosts('tupa')
def deploy(instrument=True,
           code_dir='$HOME/run',
           submit_dir='$SUBMIT_HOME',
           branch='monin'):
    print(green("Started"))
    check_code(code_dir, branch, clean=False)
    with cd(code_dir):
        with cd('Modelos/MOM4p1/exp/cpld2.1'):
            replace_vars()
            with prefix('source NAMELIST.environment'):
                if instrument:
                    with prefix('module load perftools'):
#                        clean_model_compilation()
                        compile_model()
                        instrument_code()
                else:
                    compile_model()
                prepare_expdir()
                run('mkdir -p ${rootexp}/AGCM-1.0/model')
                link_agcm_inputs()
                prepare_workdir()
            fix_submit()
            run_model()


def instrument_code():
    print(yellow('Rebuilding executable with instrumentation'))
    run('cp ~/pat/instrument_coupler.apa ${expdir}/exec/')
    run('pat_build -O ${expdir}/exec/instrument_coupler.apa -o ${executable}+apa ${executable}')
    run("sed -i.bak -r -e 's/^export executable=.*$/export executable=\$\{expdir\}\/exec\/fms_mom4p1_coupled.x+apa/g' NAMELIST.environment")


def run_model():
    print(yellow('Running model'))
    run('. run_g4c_model.cray cold 2007010100 2007011000 2007011000 48 rodteste')


def prepare_expdir():
    print(yellow('Preparing expdir'))
    run('cp -R tables ${expdir}')


def prepare_workdir():
    print(yellow('Preparing workdir'))
    run('mkdir -p ${workdir}')
    run('cp -R $ARCHIVE_OCEAN/database/work20070101/* ${workdir}')
    run('touch ${workdir}/time_stamp.restart')


def clean_model_compilation():
    print(yellow("Cleaning code dir"))
    run('./run_g4c_model.cray clean')


def compile_model():
    print(yellow("Compiling code"))
    run('mkdir -p ${expdir}/exec')
    run('./run_g4c_model.cray make')


def check_code(code_dir, branch, clean=True):
    # FIXME: sanitize input!
    print(yellow("Checking code"))
    if clean:
        run('rm -rf %s' % code_dir)
    if not exists(code_dir):
        print(yellow("Creating new repository"))
        run('hg clone $ARCHIVE_OCEAN/repos %s' % code_dir)
    with cd(code_dir):
        print(yellow("Updating existing repository"))
        run('hg pull')
        run('hg update %s' % branch)


def link_agcm_inputs():
    if not exists('${rootexp}/AGCM-1.0/model/datain'):
        print(yellow("Linking AGCM input data"))
        run('ln -s $SUBMIT_OCEAN/database/AGCM-1.0/model/datain ${rootexp}/AGCM-1.0/model/datain')


def replace_vars():
    # TODO: modificar NAMELIST.environment
    # TODO: usar upload_template em vez de sed?
    print(yellow("Replacing variables in NAMELIST.environment"))

    print(yellow("Replacing variable root"))
    run("sed -i.bak -r -e 's/^export root=.*$/export root=\$\{HOME\}\/run\/Modelos/g' NAMELIST.environment")

    print(yellow("Replacing variable rootexp"))
    run("sed -i.bak -r -e 's/^export rootexp=.*$/export rootexp=\$\{SUBMIT_HOME\}\/Modelos/g' NAMELIST.environment")

    print(yellow("Replacing variable workdir"))
    run("sed -i.bak -r -e 's/^export workdir=.*$/export workdir=\$\{WORK_HOME\}\/Modelos\/MOM4p1\/\$\{name\}\/work20070101/g' NAMELIST.environment")

    print(yellow("Replacing variable diagtable"))
    run("sed -i.bak -r -e 's/^export diagtable=.*$/export diagtable=\$\{expdir\}\/tables\/diag_table/g' NAMELIST.environment")

    print(yellow("Replacing variable fieldtable"))
    run("sed -i.bak -r -e 's/^export fieldtable=.*$/export fieldtable=\$\{expdir\}\/tables\/field_table/g' NAMELIST.environment")

    print(yellow("Replacing variable datatable"))
    run("sed -i.bak -r -e 's/^export datatable=.*$/export datatable=\$\{expdir\}\/tables\/data_table/g' NAMELIST.environment")

    run('touch tables/data_override')


def fix_submit():
    run("sed -i.bak -r -e 's/^aprun -n \$\{npes\} submit/aprun -n \$\{npes\} .\/submit/g' run_g4c_model.cray")
