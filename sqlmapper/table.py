
from __future__ import absolute_import
import re
from .utils import NoValue, validate_name, quote_key, format_func, is_bytes, is_int, is_str


class Table(object):
    def __init__(self, name, engine, keyword='%s', quote='`'):
        self.tablename = name
        self.engine = engine
        self.keyword = keyword
        self.quote = quote
    
    def cc(self, name):
        return quote_key(name, self.quote)

    @property
    def cursor(self):
        return self.engine.get_cursor()

    def describe(self):
        return self.engine.get_columns(self.tablename)

    def get_column(self, name):
        for column in self.describe():
            if column['name'] == name:
                return column

    def insert(self, data):
        keys = []
        values = []
        items = []
        for key, value in data.items():
            keys.append(self.cc(key))
            values.append(value)
            items.append(self.keyword)

        sql = 'INSERT INTO {} ({}) VALUES ({})'.format(self.cc(self.tablename), ', '.join(keys), ', '.join(items))
        self.cursor.execute(sql, tuple(values))
        assert self.cursor.rowcount == 1
        return self.cursor.lastrowid

    def _build_filter(self, filter):
        if filter is None:
            return None, []
        elif isinstance(filter, dict):
            keys = []
            values = []
            for k, v in filter.items():
                if '.' in k:
                    k = self.cc(k)
                else:
                    k = self.cc(self.tablename + '.' + k)
                if v is None:
                    keys.append(k + ' is NULL')
                else:
                    keys.append(k + '=' + self.keyword)
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
            else:
                raise ValueError('No primary key')
            return '{} = {}'.format(self.cc(key), self.keyword), [filter]
        else:
            raise NotImplementedError

    def find_one(self, filter=None, join=None, left_join=None, for_update=False, columns=None, order_by=None):
        result = list(self.find(filter, limit=1, join=join, for_update=for_update, columns=columns, order_by=order_by))
        if result:
            return result[0]

    def find(self, filter=None, limit=None, join=None, left_join=None, for_update=False, columns=None, group_by=None, order_by=None):
        """
            join='subtable.id=column'
            join='subtable as tbl.id=column'
        """

        if columns:
            assert not join
            if not isinstance(columns, (list, tuple)):
                columns = [columns]
            columns = ', '.join(map(lambda n: format_func(n, self.quote), columns))
        else:
            columns = '{}.*'.format(self.tablename)

        joins = []
        if join or left_join:
            assert bool(join) ^ bool(left_join)
            if left_join:
                join = left_join
                prefix = 'LEFT '
            else:
                prefix = ''
            r = re.match(r'(\w+)\.(\w+)=(\w+)', join)
            if r:
                table2, column2, column1 = r.groups()
                alias = table2
            else:
                r = re.match(r'(\w+)\s+as\s+(\w+)\.(\w+)=(\w+)', join)
                assert r
                table2, alias, column2, column1 = r.groups()

            columns += ', \'\' as __divider, {}.*'.format(alias)
            join = ' {}JOIN {} AS {} ON {}.{} = {}'.format(prefix, table2, alias, alias, column2, column1)
            
            key = None
            if left_join:
                for c in self.engine.get_columns(self.tablename):
                    if c['primary']:
                        key = c['name']
                        break

            joins.append({
                'alias': alias,
                'key': key
            })

        sql = 'SELECT {} FROM {}'.format(columns, self.cc(self.tablename))
        where, values = self._build_filter(filter)
        if join:
            sql += join
        if where:
            sql += ' WHERE ' + where
        if group_by:
            sql += ' GROUP BY ' + self.cc(group_by)
        if order_by:
            if not isinstance(order_by, list):
                order_by = [order_by]
            oc = []
            for name in order_by:
                if name.startswith('-'):
                    oc.append(self.cc(name[1:]) + ' DESC')
                else:
                    oc.append(self.cc(name))
            sql += ' ORDER BY ' + ', '.join(oc)
        if limit:
            assert is_int(limit)
            sql += ' LIMIT {}'.format(limit)

        if for_update:
            sql += ' FOR UPDATE'

        self.cursor.execute(sql, tuple(values))

        columns = self.cursor.description
        if self.cursor.rowcount:
            for row in self.cursor:
                join_index = -1
                join_alias = None
                join_key = None
                d = {}
                for i, value in enumerate(row):
                    col = columns[i]
                    column_name = col[0]
                    if column_name == '__divider':
                        join_index += 1
                        join_alias = joins[join_index]['alias']
                        join_key = joins[join_index]['key']
                        d[join_alias] = {}
                        continue
                    if join_alias:
                        if column_name == join_key:
                            if value is None:
                                d[join_alias] = None
                        if d[join_alias] is not None:
                            d[join_alias][column_name] = value
                    else:
                        d[column_name] = value
                yield d

    def update(self, filter=None, update=None, limit=None):
        up = []
        values = []
        for key, value in update.items():
            up.append('{} = {}'.format(self.cc(key), self.keyword))
            values.append(value)

        sql = 'UPDATE {} SET {}'.format(self.cc(self.tablename), ', '.join(up))

        where, wvalues = self._build_filter(filter)
        if where:
            sql += ' WHERE ' + where
            values += wvalues
        
        if limit:
            assert is_int(limit)
            sql += ' LIMIT {}'.format(limit)

        self.cursor.execute(sql, tuple(values))

    def update_one(self, filter=None, update=None):
        self.update(filter, update, limit=1)

    def delete(self, filter=None):
        where, values = self._build_filter(filter)

        sql = 'DELETE FROM {}'.format(self.cc(self.tablename))
        if where:
            sql += ' WHERE {}'.format(where)
        self.cursor.execute(sql, tuple(values))

    def count(self, filter=None):
        where, values = self._build_filter(filter)

        sql = 'SELECT COUNT(*) FROM {}'.format(self.cc(self.tablename))
        if where:
            sql += ' WHERE {}'.format(where)
        self.cursor.execute(sql, tuple(values))
        return self.cursor.fetchone()[0]

    def drop(self, exist_ok=True):
        sql = 'DROP TABLE '
        if exist_ok:
            sql += 'IF EXISTS '
        sql += self.tablename
        self.cursor.execute(sql)
