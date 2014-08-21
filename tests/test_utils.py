#!/usr/bin/env python

import sys

from datetime import datetime
from dateutils import relativedelta


from bosun import utils


def test_genrange_int():
    assert list(utils.genrange(1, 6, 2)) == [1, 3, 5]


def test_genrange_mixed_numbers():
    assert list(utils.genrange(1, 3., .5)) == [1, 1.5, 2, 2.5]


def test_genrange_datetime():
    begin = datetime.strptime("2008010112", "%Y%m%d%H")
    end = datetime.strptime("2008010512", "%Y%m%d%H")

    interval, units = "1 day".split()
    if not units.endswith('s'):
        units = units + 's'
    delta = relativedelta(**dict([[units, int(interval)]]))

    assert (list(utils.genrange(begin, end, delta)) ==
                    [datetime(2008, 1, 1, 12, 0), datetime(2008, 1, 2, 12, 0),
                     datetime(2008, 1, 3, 12, 0), datetime(2008, 1, 4, 12, 0)])


def test_total_seconds():
    period = datetime(2001, 1, 1) - datetime(2000, 1, 1)
    assert utils.total_seconds(period) == 31622400


def test_calc_ETA():
    assert utils.calc_ETA(4, 30, .25) == (13, 30)


def test_print_ETA():
    # TODO: how to capture and check stdout?
    pass


def test_hsm_full_path():
    environ = {
      'start': 2008012200,
      'type': 'atmos',
      'hsm': '/archive',
      'name': 'base'
    }

    canonical_names = (('atmos', 'AGCM'),
                       ('coupled', 'CGCM'),
                       ('mom4p1_falsecoupled', 'OGCM'))

    for t, n in canonical_names:
        environ['type'] = t
        full_path, cname = utils.hsm_full_path(environ)
        assert full_path == "/archive/base/dataout/ic01/ic2008/22"
        assert cname == n


def test_clear_output():
    out = """
    """
    cleaned = utils.clear_output(out)
    assert ("HOME=" not in cleaned) is True
    assert ("TRANSFER_HOME=" not in cleaned) is True
    assert ("SUBMIT_HOME=" not in cleaned) is True
    assert ("WORK_HOME=" not in cleaned) is True
