#!/usr/bin/python


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
             (tdelta.seconds + tdelta.days * 24 * 3600) * 10**6) / 10**6
