#!/usr/bin/env python

import functools
from copy import deepcopy
import re
from StringIO import StringIO

from fabric.api import run, get, cd, prefix, hide
from fabric.contrib.files import exists
import yaml


API_VERSION = 'v1'


class NoEnvironmentSetException(Exception):
    pass


class APIVersionException(Exception):
    pass


def env_options(func):
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
                       'expfiles': '${HOME}/.bosun_exps'}
            environ = _expand_config_vars(environ)
            with hide('running', 'stdout', 'stderr', 'warnings'):
                if exists(fmt('{expfiles}', environ)):
                    run(fmt('rm -rf {expfiles}', environ))
                run(fmt('hg clone {exp_repo} {expfiles}', environ))
#                else:
#                    with cd(fmt('{expfiles}', environ)):
#                        run('hg pull -u ')

                temp_exp = StringIO()
                get(fmt('{expfiles}/exp/{name}/namelist.yaml', environ),
                    temp_exp)

            kw['expfiles'] = environ['expfiles']
            environ = load_configuration(temp_exp.getvalue(), kw)
            kw.pop('expfiles', None)
            kw.pop('name', None)
            kw.pop('exp_repo', None)
            temp_exp.close()

        return func(environ, **kw)

    return functools.update_wrapper(_wrapped_env, func)


def load_configuration(yaml_string, kw=None):
    environ = yaml.safe_load(yaml_string)
    environ = _expand_config_vars(environ, updates=kw)
    environ = update_model_type(environ)

    #if environ.get('API', 0) != API_VERSION:
    #    print fc.red('Error: Configuration outdated')
    #    ref = yaml.safe_load(
    #      os.path.join(os.path.dirname(__file__), 'api.yaml'))
    #    report_differences(environ, ref)
    #    raise APIVersionException

    if environ is None:
        raise NoEnvironmentSetException
    else:
        if environ.get('ensemble', False):
            environs = []
            ensemble = environ['ensemble']
            environ.pop('ensemble')
            for member in ensemble.keys():
                new_env = update_environ(environ, ensemble, member)
                new_env = update_model_type(environ)
                environs.append(new_env)

            # TODO: ignoring for now, but need to think on how to run
            # multiple environs (one for each ensemble member).
            # Maybe use the Fabric JobQueue, which abstracts
            # multiprocessing?

    return environ


def update_model_type(environ):
    from bosun import agcm, mom4, coupled

    mtype = environ.get('type', None)
    if mtype == 'coupled':
        environ['model'] = coupled
    elif mtype == 'mom4p1_falsecoupled':
        environ['model'] = mom4
    elif mtype == 'atmos':
        environ['model'] = agcm
    else:
        environ['model'] = None

    return environ


def update_component(new_env, nml, comp):
    ''' Update component in new_env. Needed because component has nested
        definitions, and a dict.update would not recurse. '''
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
    ''' Updates an existing environ '''
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
             if not isinstance(environ.get(k, None), (dict, list, tuple))]

    env_vars = " ".join(["=".join((key, str(value)))
                         for (key, value) in environ.items()
                         if key in valid])
    return prefix("export %s" % env_vars)


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
                        var = run('if [ "${%s}" ]; then echo "${%s}"; else exit -1; fi' % (k, k)).split('\n')[-1]
                        env_vars[k] = var
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
                    all_data = dict(old_env)
                    if ref:
                        all_data.update(ref)
                    new_env[k] = env_replace(old_env[k], all_data)
            else:
                # if it is, check if we can substitute for booleans
                if old_env[k].lower() == 'false':
                    new_env[k] = False
                elif old_env[k].lower() == 'true':
                    new_env[k] = True
                else:
                    # else start replacing vars
                    new_env[k] = rec_replace(ref if ref else old_env,
                                             old_env[k])
        return new_env

    if updates:
        d.update(updates)
    new_d = env_replace(d)

    return new_d


def fmt(string, environ):
    '''String formatting and sanitization function'''
    return string.format(**environ)


def report_differences(environ, ref):
    env_keys = set(environ.keys())

    req_keys = ref[API_VERSION]['Required']

    for key in req_keys:
        if not isinstance(key, dict):
            if key not in env_keys:
                print 'missing attribute: %s' % key
        else:
            for att, values in key.items():
                for v in values:
                    try:
                        environ[att].keys()
                    except AttributeError:
                        print 'missing attribute: %s[%s]' % (att, v)
                    else:
                        if v not in environ[att].keys():
                            print 'missing attribute: %s[%s]' % (att, v)
