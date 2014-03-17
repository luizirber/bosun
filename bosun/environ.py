#!/usr/bin/env python

import functools
from copy import deepcopy
import string
from StringIO import StringIO

from fabric.api import run, get, prefix, hide, cd
from fabric.contrib.files import exists
import rec_env


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
            missing_fmt=EnvVarFormatter()
            environ = rec_env._expand_config_vars(environ,
                                                  missing_fmt=missing_fmt)
            # Temporary fix: new config format
            # uses {} instead of ${} for variables
            environ = _fix_environ(environ)

            with hide('running', 'stdout', 'stderr', 'warnings'):
                if exists(fmt('{expfiles}', environ)):
                    with cd(fmt('{expfiles}', environ)):
                        run('hg pull')
                        run('hg update -C')
                    #run(fmt('rm -rf {expfiles}', environ))
                else:
                    run(fmt('hg clone {exp_repo} {expfiles}', environ))

                temp_exp = StringIO()
                get(fmt('{expfiles}/exp/{name}/namelist.yaml', environ),
                    temp_exp)

            kw['expfiles'] = environ['expfiles']
            environ = load_configuration(temp_exp.getvalue(), kw, missing_fmt)
            kw.pop('expfiles', None)
            kw.pop('name', None)
            kw.pop('exp_repo', None)
            temp_exp.close()
            environ = _fix_environ(environ)

        return func(environ, **kw)

    return functools.update_wrapper(_wrapped_env, func)


class EnvVarFormatter(string.Formatter):
    env_vars = {}

    def get_value(self, key, args, kwargs):
        try:
            if hasattr(key, "mod"):
                return args[key]
            else:
                return kwargs[key]
        except:
            if not self.env_vars:
                with hide('running', 'stdout', 'stderr', 'warnings'):
                    env_output = run('env')
                    for line in env_output.split('\n'):
                        try:
                            k, v = line.split('=')
                        except:
                            pass
                        else:
                            self.env_vars[k] = v.rstrip()

            return self.env_vars[key]


def _fix_environ(environ):
    for k in environ:
        try:
            environ[k] = environ[k].replace('$', '')
        except:
            if isinstance(environ[k], dict):
                _fix_environ(environ[k])
            else:
                pass
    return environ


def load_configuration(yaml_string, kw=None, missing_fmt=None):
    environ = rec_env.load_configuration(yaml_string, kw, missing_fmt)
    environ = update_model_type(environ)

    #if environ.get('API', 0) != API_VERSION:
    #    print fc.red('Error: Configuration outdated')
    #    ref = yaml.safe_load(
    #      os.path.join(os.path.dirname(__file__), 'api.yaml'))
    #    report_differences(environ, ref)
    #    raise APIVersionException

    if 'ensemble' in environ:
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
