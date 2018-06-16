# coding: utf8

import MySQLdb
import threading
import re
import copy
from .utils import NoValue, validate_name, cc, cc2, is_bytes, is_int, is_str


class Engine(object):
    def __init__(self, **kw):
        autocreate = kw.pop('autocreate', False)
        self.read_commited = kw.pop('read_commited', False)
        self.local = threading.local()
        if 'charset' not in kw:
            kw['charset'] = 'utf8mb4'

        try:
            conn = MySQLdb.connect(**kw)
        except MySQLdb.OperationalError as e:
            if autocreate and e.args[0] == 1049:
                ckw = kw.copy()
                db = ckw.pop('db')

                conn = MySQLdb.connect(**ckw)
                cursor = conn.cursor()
                cursor.execute('CREATE DATABASE {}'.format(db))
                conn.close()
                conn = MySQLdb.connect(**kw)
            else:
                raise
        self.local.tables = {}
        self.local.conn = conn

    def commit(self):
        self.local.conn.commit()
    
    def rollback(self):
        self.local.conn.rollback()

    def close(self):
        self.local.conn.close()
        self.local.cursor = None
        self.local.conn = None

    def get_cursor(self):
        if hasattr(self.local, 'cursor'):
            return self.local.cursor
        
        self.local.cursor = cursor = self.local.conn.cursor()
        if self.read_commited:
            cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        return cursor

    def get_table(self, name):
        return Table(name, self)
    
    def get_tables(self):
        cursor = self.get_cursor()

        cursor.execute('SHOW TABLES')
        for row in cursor:
            yield row[0]


class Table(object):
    def __init__(self, name, engine):
        self.tablename = name
        self.engine = engine

    @property
    def cursor(self):
        return self.engine.get_cursor()

    def describe(self):
        result = self.engine.local.tables.get(self.tablename)
        if not result:
            result = []
            self.cursor.execute('describe `{}`'.format(self.tablename))
            for row in self.cursor:
                result.append({
                    'name': row[0],
                    'type': row[1],
                    'null': row[2] == 'YES',
                    'default': row[4],
                    'primary': row[3] == 'PRI',
                    'auto_increment': row[5] == 'auto_increment'
                })
            self.engine.local.tables[self.tablename] = result
        return copy.deepcopy(result)

    def get_column(self, name):
        for column in self.describe():
            if column['name'] == name:
                return column

    def add_column(self, name, column_type, not_null=False, default=NoValue, exist_ok=False, primary=False, auto_increment=False, collate=None):
        validate_name(name)
        assert re.match(r'^[\w\d\(\)]+$', column_type), 'Wrong type: {}'.format(column_type)
        values = []
        scolumn = '`{}` {}'.format(name, column_type)

        if collate:
            charset = collate.split('_')[0]
            scolumn += ' CHARACTER SET {} COLLATE {}'.format(charset, collate)

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

        if self.tablename in self.engine.get_tables():
            if exist_ok:
                if self.get_column(name):
                    return
            if primary:
                scolumn += ', ADD PRIMARY KEY (`{}`)'.format(name)
            sql = 'ALTER TABLE `{}` ADD COLUMN {}'.format(self.tablename, scolumn)
        else:
            if primary:
                scolumn += ', PRIMARY KEY (`{}`)'.format(name)
            collate = collate or 'utf8mb4_unicode_ci'
            charset = collate.split('_')[0]
            sql = 'CREATE TABLE `{}` ({}) ENGINE=InnoDB DEFAULT CHARSET {} COLLATE {}'.format(self.tablename, scolumn, charset, collate)
        self.cursor.execute(sql, tuple(values))
        self.engine.local.tables[self.tablename] = None

    def insert(self, data):
        keys = []
        values = []
        items = []
        for key, value in data.items():
            keys.append(cc(key))
            values.append(value)
            items.append('%s')

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
            else:
                raise ValueError('No primary key')
            return '`{}` = %s'.format(key), [filter]
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
            up.append('`{}` = %s'.format(key))
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
