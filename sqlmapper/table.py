# coding: utf8

from __future__ import absolute_import
from .utils import validate_name, cc, cc2, is_bytes, is_int, is_str


class Table(object):
    def __init__(self, engine, table, param='%s'):
        validate_name(table)
        self.engine = engine
        self.table = table
        self.param = param

    def _build_filter(self, filter):
        if filter is None:
            return None, []
        elif isinstance(filter, dict):
            keys = []
            values = []
            for k, v in filter.items():
                if '.' in k:
                    k = '.'.join(map(cc, k.split('.')))
                else:
                    k = '`{}`.{}'.format(self.table, cc(k))
                if v is None:
                    keys.append(k + ' is NULL')
                else:
                    keys.append(k + '=' + self.param)
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
            return '`{}` = {}'.format(key, self.param), [filter]
        else:
            raise NotImplementedError

    def find_one(self, filter=None, join=None, for_update=False, columns=None):
        return list(self.find(filter, limit=1, join=join, for_update=for_update, columns=columns))[0]

    def find(self, filter=None, limit=None, join=None, for_update=False, columns=None, group_by=None):
        """
            join='subtable.id=column'
            join='subtable as tbl.id=column'
        """

        if columns:
            assert not join
            if not isinstance(columns, (list, tuple)):
                columns = [columns]
            columns = ', '.join(map(cc2, columns))
        else:
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
        if group_by:
            sql += ' GROUP BY ' + cc(group_by)
        if limit:
            assert isinstance(limit, int)
            sql += ' LIMIT {}'.format(limit)

        if for_update:
            sql += ' FOR UPDATE'

        cursor = self.engine.cursor
        cursor.execute(sql, tuple(values))

        columns = cursor.description
        if cursor.rowcount:
            for row in cursor:
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
            up.append('`{}` = {}'.format(key, self.param))
            values.append(value)

        sql = 'UPDATE `{}` SET {}'.format(self.table, ', '.join(up))

        where, wvalues = self._build_filter(filter)
        if where:
            sql += ' WHERE ' + where
            values += wvalues

        self.engine.cursor.execute(sql, tuple(values))

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
            items.append(self.param)

        cursor = self.engine.cursor
        sql = 'INSERT INTO `{}` ({}) VALUES ({})'.format(self.table, ', '.join(keys), ', '.join(items))
        cursor.execute(sql, tuple(values))
        assert cursor.rowcount == 1
        return cursor.lastrowid

    def delete(self, filter=None):
        where, values = self._build_filter(filter)

        sql = 'DELETE FROM `{}`'.format(self.table)
        if where:
            sql += ' WHERE {}'.format(where)
        self.engine.cursor.execute(sql, tuple(values))

    def drop(self):
        raise NotImplementedError

    def add_column(self, name, type, exist_ok=False, **kw):
        validate_name(name)
        return self.engine.add_column(self.table, name=name, type=type, exist_ok=exist_ok, **kw)

    def describe(self):
        return self.engine.get_table_details(self.table)

    def get_column(self, name):
        return self.engine.get_column(self.table, name)
