
from __future__ import absolute_import
import re
from .utils import NoValue, validate_name, cc, cc2, is_bytes, is_int, is_str


class Table(object):
    def __init__(self, name, engine, keyword='%s'):
        self.tablename = name
        self.engine = engine
        self.keyword = keyword

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
            keys.append(cc(key))
            values.append(value)
            items.append(self.keyword)

        sql = 'INSERT INTO `{}` ({}) VALUES ({})'.format(self.tablename, ', '.join(keys), ', '.join(items))
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
                    k = '.'.join(map(cc, k.split('.')))
                else:
                    k = '`{}`.{}'.format(self.tablename, cc(k))
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
            return '`{}` = {}'.format(key, self.keyword), [filter]
        else:
            raise NotImplementedError

    def find_one(self, filter=None, join=None, for_update=False, columns=None):
        result = list(self.find(filter, limit=1, join=join, for_update=for_update, columns=columns))
        if result:
            return result[0]

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
            columns = '{}.*'.format(self.tablename)

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

        sql = 'SELECT {} FROM `{}`'.format(columns, self.tablename)
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
            up.append('`{}` = {}'.format(key, self.keyword))
            values.append(value)

        sql = 'UPDATE `{}` SET {}'.format(self.tablename, ', '.join(up))

        where, wvalues = self._build_filter(filter)
        if where:
            sql += ' WHERE ' + where
            values += wvalues

        self.cursor.execute(sql, tuple(values))

    def delete(self, filter=None):
        where, values = self._build_filter(filter)

        sql = 'DELETE FROM `{}`'.format(self.tablename)
        if where:
            sql += ' WHERE {}'.format(where)
        self.cursor.execute(sql, tuple(values))

    def create_index(self, name, column, primary=False, unique=False, fulltext=False, exist_ok=False):
        if primary:
            name = 'PRIMARY'
        if exist_ok and self.has_index(name):
            return

        if isinstance(column, list):
            column = ', '.join(map(cc, column))
        else:
            column = cc(column)

        index_type = 'INDEX '
        if primary:
            index_type = 'PRIMARY KEY '
            assert not fulltext
            name = ''
        else:
            name = cc(name)

        if unique and not primary:
            assert not fulltext
            index_type = 'UNIQUE '
        elif fulltext:
            index_type = 'FULLTEXT '

        sql = 'ALTER TABLE `{}` ADD {}{}({})'.format(self.tablename, index_type, name, column)
        self.cursor.execute(sql)

    def has_index(self, name):
        self.cursor.execute('show index from ' + self.tablename)
        for row in self.cursor:
            if row[2] == name:
                return True
        return False

    def count(self, filter=None):
        where, values = self._build_filter(filter)

        sql = 'SELECT COUNT(*) FROM `{}`'.format(self.tablename)
        if where:
            sql += ' WHERE {}'.format(where)
        self.cursor.execute(sql, tuple(values))
        return self.cursor.fetchone()[0]
