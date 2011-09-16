#!/usr/bin/env python

from __future__ import with_statement
import re
import os.path

from fabric.api import run, cd, prefix, put
import fabric.colors as fc
from fabric.contrib.files import exists
from fabric.decorators import task
import yaml


__all__ = ['prepare_expdir', 'check_code', 'instrument_code', 'compile_model',
           'link_agcm_inputs', 'prepare_workdir', 'run_model', 'env_options']


class NoEnvironmentSetException(Exception):
    pass


def env_options(f):
    def _wrapped_env(*args, **kw):
        exppath = kw.get('exppath', 'exp')
        filename = kw.get('filename', 'namelist.yaml')

        if args:
            environ = args[0]
        else:
            environ = _read_config(os.path.join(exppath, filename), updates=kw)

        if environ is None:
            raise NoEnvironmentSetException
        else:
            environ['exppath'] = exppath
            environ['filename'] = filename
            return f(environ, **kw)

    return _wrapped_env


def shell_env(args):
    env_vars = " ".join(["=".join((key, str(value))) for (key, value)
                                                     in args.items()])
    return prefix("export %s" % env_vars)


def fmt(s, e):
    return s.format(**e)


def _read_config(filename, updates=None):

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
                old_env[k].split(' ')
            except AttributeError:
                new_env[k] = old_env[k]
            else:
                new_env[k] = rec_replace(old_env, old_env[k])
        return new_env

    data = open(filename, 'r').read()

    d = yaml.load(data)
    if updates:
        d.update(updates)
    new_d = env_replace(d)

    # FIXME: sanitize input!
    return new_d


@env_options
@task
def instrument_code(environ, **kwargs):
    print(fc.yellow('Rebuilding executable with instrumentation'))
    with prefix('module load perftools'):
        clean_model_compilation(environ)
        compile_model(environ)
        run(fmt('pat_build -O {expdir}/instrument_coupler.apa '
                '-o {executable}+apa {executable}', environ))
        environ['executable'] = fmt('{executable}+apa', environ)


@env_options
@task
def run_model(environ, **kwargs):
    print(fc.yellow('Running model'))
    with shell_env(environ):
        with cd(fmt('{expdir}/runscripts', environ)):
            run(fmt('. run_g4c_model.cray {mode} {start} {restart} '
                    '{finish} 48 {name}', environ))


@env_options
@task
def prepare_expdir(environ, **kwargs):
    print(fc.yellow('Preparing expdir'))
    run(fmt('mkdir -p {execdir}', environ))
    # FIXME: hack to get remote path. Seems put can't handle shell env vars in
    # remote_path
    remote_path = str(run(fmt('echo {expdir}', environ))).split('\n')[-1]
    put(fmt('{exppath}/*', environ), remote_path, mirror_local_mode=True)


@env_options
@task
def prepare_workdir(environ, **kwargs):
    print(fc.yellow('Preparing workdir'))
    run(fmt('mkdir -p {workdir}', environ))
    run(fmt('cp -R {workdir_template}/* {workdir}', environ))
    run(fmt('touch {workdir}/time_stamp.restart', environ))


@env_options
@task
def clean_model_compilation(environ, **kwargs):
    print(fc.yellow("Cleaning code dir"))
    with shell_env(environ):
        with cd(fmt('{execdir}', environ)):
            with prefix(fmt('source {envconf}', environ)):
                run(fmt('make -f {makeconf} clean', environ))


@env_options
@task
def compile_model(environ, **kwargs):
    print(fc.yellow("Compiling code"))
    with shell_env(environ):
        with prefix(fmt('source {envconf}', environ)):
            with cd(fmt('{execdir}', environ)):
                #TODO: generate RUNTM and substitute
                run(fmt('make -f {makeconf}', environ))
            with cd(environ['comb_exe']):
                run(fmt('make -f {comb_src}/Make_combine', environ))
            with cd(environ['posgrib_src']):
                fix_posgrib_makefile(environ)
                run(fmt('mkdir -p {PATH2}', environ))
                run(fmt('make cray', environ))


@env_options
@task
def fix_posgrib_makefile(environ, **kwargs):
    run("sed -i.bak -r -e 's/^PATH2/#PATH2/g' Makefile")


@env_options
@task
def check_code(environ, **kwargs):
    print(fc.yellow("Checking code"))
    if environ['clean_checkout']:
        run(fmt('rm -rf {code_dir}', environ))
    if not exists(environ['code_dir']):
        print(fc.yellow("Creating new repository"))
        run(fmt('hg clone {code_repo} {code_dir}', environ))
    with cd(environ['code_dir']):
        print(fc.yellow("Updating existing repository"))
        run('hg pull')
        run(fmt('hg update {code_branch}', environ))
#        run(fmt('hg update -r{revision}', environ))


@env_options
@task
def link_agcm_inputs(environ, **kwargs):
    for d in ['model', 'pos']:
        run(fmt('mkdir -p {rootexp}/AGCM-1.0/%s' % d, environ))
        if not exists(fmt('{rootexp}/AGCM-1.0/%s/datain' % d, environ)):
            print(fc.yellow(fmt("Linking AGCM %s input data" % d, environ)))
            if not exists(fmt('{agcm_%s_inputs}' % d, environ)):
                run(fmt('mkdir -p {agcm_%s_inputs}' % d, environ))
                run(fmt('cp -R $ARCHIVE_OCEAN/database/AGCM-1.0/%s/datain '
                        '{agcm_%s_inputs}' % (d, d), environ))
            run(fmt('ln -s {agcm_%s_inputs} '
                    '{rootexp}/AGCM-1.0/%s/datain' % (d, d), environ))
