# coding: utf8

import sys
from contextlib import contextmanager
import copy
import re
import threading


__version__ = '0.2.2'
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


def validate_name(*names):
    for name in names:
        cc(name)


def Connection(*argv, **kargs):
    pool = threading.local()
    g_mapper = Mapper

    if argv and hasattr(argv[0], 'cursor'):
        pool.connection = argv[0]

    if 'read_commited' in kargs:
        g_read_commited = kargs.pop('read_commited')

    if 'mapper' in kargs:
        g_mapper = kargs.pop('mapper')

    @contextmanager
    def mapper(read_commited=None, commit=True, mapper=None):
        if hasattr(pool, 'connection'):
            connection = pool.connection
        else:
            import MySQLdb
            pool.connection = connection = MySQLdb.connect(*argv, **kargs)

        cursor = connection.cursor()
        if read_commited or (read_commited is None and g_read_commited):
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

        commited = False
        mapper = mapper or g_mapper
        try:
            yield mapper(cursor)
            if commit:
                connection.commit()
                commited = True
        finally:
            if not commited:
                connection.rollback()
            cursor.close()

    return mapper


class Mapper(object):
    def __init__(self, cursor=None):
        self.cursor = cursor
        self._table = {}

    def __getattr__(self, name):
        if name == 'cursor':
            return self.__dict__['cursor']
        return Table(self, name)

    def show_tables(self):
        self.cursor.execute('SHOW TABLES')
        for row in self.cursor:
            yield row[0]


class Table(object):
    def __init__(self, mapper, table):
        validate_name(table)
        self.mapper = mapper
        self.cursor = mapper.cursor
        self.table = table

    def _build_filter(self, filter):
        if filter is None:
            return None, []
        elif isinstance(filter, dict):
            keys = []
            values = []
            for k, v in filter.items():
                if '.' not in k:
                    k = '`{}`.{}'.format(self.table, cc(k))
                else:
                    k = cc(k)
                if v is None:
                    keys.append(k + ' is NULL')
                else:
                    keys.append(k + '=%s')
                    values.append(v)
            sql = ' AND '.join(keys)
            return sql, values
        elif isinstance(filter, (list, tuple)):
            return filter[0], filter[1:]
        elif is_int(filter) or is_str(filter) or is_bytes(filter):
            # find by primary key
            key = None
            for column in self.describe():
                if column['primary']:
                    key = column['name']
                    break
            if not key:
                raise ValueError('No primary key')
            return '`{}` = %s'.format(key), [filter]
        else:
            raise NotImplementedError

    def find_one(self, filter=None, join=None, for_update=False):
        for row in self.find(filter, limit=1, join=join, for_update=for_update):
            return row

    def find(self, filter=None, limit=None, join=None, for_update=False):
        """
            join='subtable.id=column'
            join='subtable as tbl.id=column'
        """
        columns = '{}.*'.format(self.table)
        joins = []
        if join:
            r = re.match(r'(\w+)\.(\w+)=(\w+)', join)
            if r:
                table2, column2, column1 = r.groups()
                alias = table2
            else:
                r = re.match(r'(\w+)\s+as\s+(\w+)\.(\w+)=(\w+)', join)
                assert r
                table2, alias, column2, column1 = r.groups()

            columns += ', "" as __divider, {}.*'.format(alias)
            join = ' JOIN {} AS {} ON {}.{} = {}'.format(table2, alias, alias, column2, column1)
            joins.append(alias)

        sql = 'SELECT {} FROM `{}`'.format(columns, self.table)
        where, values = self._build_filter(filter)
        if join:
            sql += join
        if where:
            sql += ' WHERE ' + where
        if limit:
            assert isinstance(limit, int)
            sql += ' LIMIT {}'.format(limit)

        if for_update:
            sql += ' FOR UPDATE'

        self.cursor.execute(sql, tuple(values))

        columns = self.cursor.description
        if self.cursor.rowcount:
            for row in self.cursor:
                subindex = -1
                subobject = None
                d = {}
                for i, value in enumerate(row):
                    col = columns[i]
                    column_name = col[0]
                    if column_name == '__divider':
                        subindex += 1
                        d[joins[subindex]] = subobject = {}
                        continue
                    if subobject is not None:
                        subobject[column_name] = value
                    else:
                        d[column_name] = value
                yield d

    def update(self, filter=None, update=None):
        up = []
        values = []
        for key, value in update.items():
            up.append('`{}` = %s'.format(key))
            values.append(value)

        sql = 'UPDATE `{}` SET {}'.format(self.table, ', '.join(up))

        where, wvalues = self._build_filter(filter)
        if where:
            sql += ' WHERE ' + where
            values += wvalues

        self.cursor.execute(sql, tuple(values))

    def update_one(self, filter=None, update=None):
        # self.cursor.rowcount
        raise NotImplementedError

    def insert(self, data):
        keys = []
        values = []
        items = []
        for key, value in data.items():
            keys.append(cc(key))
            values.append(value)
            items.append('%s')

        sql = 'INSERT INTO `{}` ({}) VALUES ({})'.format(self.table, ', '.join(keys), ', '.join(items))
        self.cursor.execute(sql, tuple(values))
        assert self.cursor.rowcount == 1
        return self.cursor.lastrowid

    def delete(self, filter=None):
        where, values = self._build_filter(filter)

        sql = 'DELETE FROM `{}`'.format(self.table)
        if where:
            sql += ' WHERE {}'.format(where)
        self.cursor.execute(sql, tuple(values))

    def drop(self):
        raise NotImplementedError

    def create_index(self, name, columns, unique=False, exist_ok=False):
        if exist_ok and self.has_index(name):
            return
        columns = ', '.join(map(cc, columns))
        sql = 'ALTER TABLE `{}` ADD{} {}({})'.format(self.table, ' UNIQUE' if unique else '', cc(name), columns)
        self.cursor.execute(sql)

    def has_index(self, name):
        self.cursor.execute('show index from ' + self.table)
        for row in self.cursor:
            if row[2] == name:
                return True
        return False

    def add_column(self, name, type, not_null=False, default=NoValue, exist_ok=False, primary=False, auto_increment=False):
        validate_name(name)
        assert re.match(r'^[\w\d\(\)]+$', type), 'Wrong type: {}'.format(type)
        values = []
        scolumn = '`{}` {}'.format(name, type)
        if primary:
            not_null = True
        if not_null:
            scolumn += ' NOT NULL'
            if auto_increment:
                scolumn += ' AUTO_INCREMENT'
        if default != NoValue:
            if not_null or primary:
                raise ValueError('Can''t have default value')
            scolumn += ' DEFAULT %s'
            values.append(default)

        if self.table in self.mapper.show_tables():
            if exist_ok:
                if self.get_column(name):
                    return
            if primary:
                scolumn += ', ADD PRIMARY KEY (`{}`)'.format(name)
            sql = 'ALTER TABLE `{}` ADD COLUMN {}'.format(self.table, scolumn)
        else:
            if primary:
                scolumn += ', PRIMARY KEY (`{}`)'.format(name)
            sql = 'CREATE TABLE `{}` ({}) ENGINE=InnoDB DEFAULT CHARSET=utf8'.format(self.table, scolumn)
        self.cursor.execute(sql, tuple(values))

    def describe(self):
        if self.table not in self.mapper._table:
            result = []
            self.cursor.execute('describe `{}`'.format(self.table))
            for row in self.cursor:
                result.append({
                    'name': row[0],
                    'type': row[1],
                    'null': row[2] == 'YES',
                    'default': row[4],
                    'primary': row[3] == 'PRI',
                    'auto_increment': row[5] == 'auto_increment'
                })
            self.mapper._table[self.table] = result
        return copy.deepcopy(self.mapper._table[self.table])

    def get_column(self, name):
        for column in self.describe():
            if column['name'] == name:
                return column
