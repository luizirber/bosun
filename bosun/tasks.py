 #!/usr/bin/env python

from __future__ import with_statement
from __future__ import print_function
import re
import time
from datetime import timedelta, datetime
import sys

from fabric.api import run, cd, prefix, settings, hide
import fabric.colors as fc
from fabric.contrib.files import exists
from fabric.decorators import task
from dateutil.relativedelta import relativedelta

from bosun import agcm
from bosun import coupled
from bosun import mom4
from bosun.environ import env_options, fmt
from bosun.utils import total_seconds, genrange


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


GET_STATUS_SLEEP_TIME = 60


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

        check_restart(environ)

        # TODO: set months and days? only use days?
        if period.day == finish.day:
            environ['months'] = (12 * (finish.year - period.year) +
                                 finish.month - period.month)
        else:
            environ['days'] = (finish - period).days

        # TODO: set restart_interval in input.nml to be equal to delta

        if environ['type'] in ('atmos', 'coupled'):
            agcm.prepare_namelist(environ)
        if environ['type'] in ('mom4p1_falsecoupled', 'coupled'):
            mom4.prepare_namelist(environ)

        if environ['type'] == 'atmos':
            agcm.run_model(environ)
        elif environ['type'] == 'mom4p1_falsecoupled':
            mom4.run_model(environ)
        elif environ['type'] == 'coupled':
            coupled.run_model(environ)
        else:
            print(fc.red(fmt('Unrecognized type: {type}', environ)))
            sys.exit(1)

        if environ['type'] in ('mom4p1_falsecoupled', 'coupled'):
            mom4.run_post(environ)
        if environ['type'] in ('atmos', 'coupled'):
            agcm.run_post(environ)

        while check_status(environ, oneshot=True):
            time.sleep(GET_STATUS_SLEEP_TIME)

        prepare_restart(environ)
        environ['mode'] = 'warm'


@task
@env_options
def check_restart(environ, **kwargs):
    if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
        #TODO: check if coupler.res date is the same as environ['restart']
        if exists(fmt('{workdir}/INPUT/coupler.res', environ)):
            res_time = run(fmt('tail -1 {workdir}/INPUT/coupler.res', environ))
            date_comp = [i for i in res_time.split('\n')[-1]
                         .split(' ') if i][:4]
            res_date = int("".join(
                date_comp[0:1] + ["%02d" % int(i) for i in date_comp[1:4]]))
            if 'cold' in environ['mode']:
                if res_date != int(environ['start']):
                    print(fc.red('ERROR'))
                    sys.exit(1)
            else:
                if res_date != int(environ['restart']):
                    print(fc.red('ERROR'))
                    sys.exit(1)
    if environ['type'] in ('coupled', 'atmos'):
        if 'warm' in environ['mode']:
            run(fmt('ls {workdir}/model/dataout/TQ{TRC:04d}L{LV:03d}/*{start}{restart}F.unf*outatt*', environ))


@task
@env_options
def copy_restart(environ, **kwargs):
    if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
        run(fmt('cp {restart_dir}/{restart}.tar.gz', environ))
    if environ['type'] in ('coupled', 'atmos'):
        pass


@task
@env_options
def prepare_restart(environ, **kwargs):
    '''Prepare restart for new run

    Used vars:
      type
      workdir

    Depends on:
      None
    '''
    if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
        # copy ocean restarts for the day to ${workdir}/INPUT

        #restart_init = environ['finish'][0:8]
        #files = run(fmt('ls {workdir}/RESTART/*' % restart_init, environ))
        #for rfile in files:
        #    run(fmt('cp {workdir}/RESTART/%s {workdir}/INPUT/%s' %
        #        (rfile, rfile.split('.')[2:]), environ))
        run(fmt('rsync -rtL --progress {workdir}/RESTART/* {workdir}/INPUT/',
            environ))
    if environ['type'] in ('coupled', 'atmos'):
        # for now running in same dir, so no need to copy atmos restarts
        # (but it's a good thing to do).
        pass


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


def _calc_ETA(remh, remm, percent):
    ''' Calculate estimated remaining time '''
    diff = remh * 60 + remm
    minutes = diff / (percent or 1)
    remain = timedelta(minutes=minutes) - timedelta(minutes=diff)
    return remain.days / 24 + remain.seconds / 3600, (remain.seconds / 60) % 60


def _handle_mainjob(environ, status):
    ''' Handle mainjob status '''
    logfile = fmt("{workdir}/logfile.000000.out", environ)
    fmsfile = fmt("{workdir}/fms.out", environ)
    if status['S'] == 'R':
        if not exists(logfile):
            print(fc.yellow('Preparing!'))
        else:
            try:
                if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
                    line = run(
                        'tac %s | grep -m1 yyyy' % fmsfile).split('\n')[-1]
                    current = re.search('(\d{4})/(\s*\d{1,2})/(\s*\d{1,2})\s(\s*\d{1,2}):(\s*\d{1,2}):(\s*\d{1,2})', line)
                    try:
                        current = datetime(*[int(i) for i in current.groups()])
                    except AttributeError:
                        print(fc.yellow('Preparing!'))
                    else:
                        if environ['mode'] == 'warm':
                            start = environ['restart']
                        else:
                            start = environ['start']
                        begin = datetime.strptime(str(start), "%Y%m%d%H")
                        end = (datetime.strptime(
                               str(environ['finish']), "%Y%m%d%H")
                               + relativedelta(days=+1))
                        count = current - begin
                        total = end - begin
                        percent = (float(total_seconds(count))
                                   / total_seconds(total))

                        runh, runm = map(float, status['Time'].split(':'))
                        remh, remm = _calc_ETA(runh, runm, percent)
                        print(fc.yellow('Model running time: %s, %.2f %% completed, Estimated %02d:%02d'
                              % (status['Time'], 100 * percent, remh, remm)))
                else:  # TODO: how to calculate that for atmos?
                    pass
            except:  # ignore all errors in this part
                pass
            else:
                print(fc.yellow('Model: %s' % JOB_STATES[status['S']]))
    else:
        print(fc.yellow('Model: %s' % JOB_STATES[status['S']]))


@task
@env_options
def check_status(environ, **kwargs):
    print(fc.yellow('Checking status'))
    statuses = _get_status(environ)
    if statuses:
        while statuses:
            for status in sorted(statuses.values(), key=lambda x: x['ID']):
                if status['ID'] in environ.get('JobID_model', ""):
                    _handle_mainjob(environ, status)
                elif status['ID'] in environ.get('JobID_pos_ocean', ""):
                    print(fc.yellow('Ocean post-processing: %s' %
                          JOB_STATES[status['S']]))
                elif status['ID'] in environ.get('JobID_pos_atmos', ""):
                    print(fc.yellow('Atmos post-processing: %s' %
                          JOB_STATES[status['S']]))
            if kwargs.get('oneshot', False):
                statuses = {}
            else:
                time.sleep(GET_STATUS_SLEEP_TIME)
                statuses = _get_status(environ)
            print()
        return 1
    else:
        print(fc.yellow('No jobs running.'))
        return 0


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
    if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
        run(fmt('mkdir -p {comb_exe}', environ))
        if environ.get('gengrid_run_this_module', False):
            run(fmt('mkdir -p {execdir}/gengrid', environ))
            run(fmt('mkdir -p {gengrid_workdir}', environ))
        if environ.get('make_xgrids_run_this_module', False):
            run(fmt('mkdir -p {execdir}/make_xgrids', environ))
            run(fmt('mkdir -p {make_xgrids_workdir}', environ))
        if environ.get('regrid_3d_run_this_module', False):
            run(fmt('mkdir -p {execdir}/regrid_3d', environ))
            run(fmt('mkdir -p {regrid_3d_workdir}', environ))
        if environ.get('regrid_2d_run_this_module', False):
            run(fmt('mkdir -p {execdir}/regrid_2d', environ))
            run(fmt('mkdir -p {regrid_2d_workdir}', environ))
        # Need to check if input.nml->ocean_drifters_nml->use_this_module is True
        run(fmt('mkdir -p {workdir}/DRIFTERS', environ))
    if environ['type'] in 'atmos':
        run(fmt('mkdir -p {PATH2}', environ))
    run(fmt('rsync -rtL --progress {expfiles}/exp/{name}/* {expdir}', environ))


@task
@env_options
def clean_experiment(environ, **kwargs):
    run(fmt('rm -rf {expdir}', environ))
    run(fmt('rm -rf {rootexp}', environ))
    run(fmt('rm -rf {execdir}', environ))
    if environ['type'] in ('coupled', 'mom4p1_falsecoupled'):
        run(fmt('rm -rf {comb_exe}', environ))
    if environ['type'] in 'atmos':
        run(fmt('rm -rf {PATH2}', environ))
    run(fmt('rm -rf {workdir}', environ))


@task
@env_options
def prepare_workdir(environ, **kwargs):
    '''Prepare output dir

    Used vars:
      workdir
      workdir_template
    '''
    print(fc.yellow('Preparing workdir'))
    run(fmt('mkdir -p {workdir}', environ))
    run(fmt('rsync -rtL --progress {workdir_template}/* {workdir}', environ))
    run(fmt('touch {workdir}/time_stamp.restart', environ))
    # TODO: lots of things.
    #  1) generate atmos inputs (gdas_to_atmos from oper scripts)
    #  2) copy restart files from somewhere (emanuel's spinup, for example)


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
    if environ['type'] == 'mom4p1_falsecoupled':
        mom4.compile_model(environ)
        mom4.compile_post(environ)
    elif environ['type'] == 'atmos':
        agcm.compile_model(environ)
        agcm.compile_pre(environ)
        agcm.compile_post(environ)
    elif environ['type'] == 'coupled':
        coupled.compile_model(environ)
        mom4.compile_post(environ)
        agcm.compile_pre(environ)
        agcm.compile_post(environ)


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
