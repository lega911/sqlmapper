# coding: utf8

import sys
import re


PY3 = sys.version_info.major == 3
NoValue = object()

if PY3:
    def is_int(value):
        return isinstance(value, int)

    def is_str(value):
        return isinstance(value, str)

    def is_bytes(value):
        return isinstance(value, bytes)

else:
    def is_int(value):
        return isinstance(value, (int, long))

    def is_str(value):
        return isinstance(value, unicode)

    def is_bytes(value):
        return isinstance(value, str)


def cc(name):
    assert is_str(name) or is_bytes(name), 'Wrong type'
    assert re.match(r'^[\w\d_]+$', name), 'Wrong name value: `{}`'.format(name)
    return '`' + name + '`'


def cc2(name):
    r = re.match(r'^(\w+)\(([^\)]+)\)$', name)
    if not r:
        return cc(name)

    func = r.groups(0)[0]
    name = r.groups(0)[1]
    return '{}({}) as {}'.format(func, cc(name), cc(func+'_'+name).lower())


def validate_name(*names):
    for name in names:
        cc(name)
