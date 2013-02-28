#!/usr/bin/python

from datetime import timedelta, datetime

from dateutil.relativedelta import relativedelta
import fabric.colors as fc


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


def genrange(*args):
    ''' Replacement for the range() builtin, accepting any argument that can be
        compared (like datetimes and timedeltas).'''

    if len(args) != 3:
        return range(*args)
    start, stop, step = args
    if start < stop:
        cmpe = lambda a, b: a < b
        inc = lambda a: a + step
    else:
        cmpe = lambda a, b: a > b
        inc = lambda a: a - step
    output = []
    while cmpe(start, stop):
        output.append(start)
        start = inc(start)
    return output


def total_seconds(tdelta):
    ''' Returns total seconds, given a timedelta '''
    return (tdelta.microseconds +
            (tdelta.seconds + tdelta.days * 24 * 3600) * 10 ** 6) / 10 ** 6


def calc_ETA(remh, remm, percent):
    ''' Calculate estimated remaining time '''
    diff = remh * 60 + remm
    minutes = diff / (percent or 1)
    remain = timedelta(minutes=minutes) - timedelta(minutes=diff)
    return remain.days / 24 + remain.seconds / 3600, (remain.seconds / 60) % 60


def print_ETA(environ, status, current):
    if environ['mode'] == 'warm':
        start = environ['restart']
    else:
        start = environ['start']
    begin = datetime.strptime(str(start), "%Y%m%d%H")
    end = (datetime.strptime(
        str(environ['finish']), "%Y%m%d%H") + relativedelta(days=+1))
    count = current - begin
    total = end - begin
    percent = (float(total_seconds(count)) / total_seconds(total))

    runh, runm = map(float, status['Time'].split(':'))
    remh, remm = calc_ETA(runh, runm, percent)
    print(fc.yellow('Model running time: %s, %.2f %% '
                    'completed, Estimated %02d:%02d'
                    % (status['Time'], 100 * percent, remh, remm)))
