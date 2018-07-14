
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


def validate_name(name):
    assert name
    assert is_str(name) or is_bytes(name), 'Wrong type'
    assert re.match(r'^[\w\d_]+$', name), 'Wrong name value: `{}`'.format(name)


def quote_key(name, q='`'):
    if '.' in name:
        name = name.split('.')
    else:
        name = [name]

    result = []
    for n in name:
        validate_name(n)
        result.append(q + n + q)
    return '.'.join(result)


def format_func(name, q='`'):
    r = re.match(r'^(\w+)\(([^\)]+)\)$', name)
    if not r:
        return quote_key(name, q)

    func = r.groups(0)[0]
    name = r.groups(0)[1]
    return '{}({}) as {}'.format(func, quote_key(name, q), quote_key(func + '_' + name, q).lower())
