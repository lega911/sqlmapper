# coding: utf8

from __future__ import absolute_import
import re
from .utils import validate_name, add_engine, cc
from .table import Table


class MysqlEngine(object):
    def __init__(self, option):
        import MySQLdb
        opt = {}
        for k in ['db', 'host', 'port', 'user', 'password', 'charset']:
            if k in option:
                opt[k] = option[k]

        if 'charset' not in opt:
            opt['charset'] = 'utf8mb4'

        self.connection = MySQLdb.connect(**opt)
        self.cursor = self.connection.cursor()

        if option.get('read_commited'):
            self.cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    def table(self, name):
        return MysqlTable(self, name)

    def get_tables(self):
        self.cursor.execute('SHOW TABLES')
        for row in self.cursor:
            yield row[0]

    def describe(self, table):
        result = []
        self.cursor.execute('describe `{}`'.format(table))
        for row in self.cursor:
            result.append({
                'name': row[0],
                'type': row[1],
                'null': row[2] == 'YES',
                'default': row[4],
                'primary': row[3] == 'PRI',
                'auto_increment': row[5] == 'auto_increment'
            })
        return result

    def get_column(self, table, name):
        for column in self.describe(table):
            if column['name'] == name:
                return column

    def commit(self):
        return self.connection.commit()

    def rollback(self):
        return self.connection.rollback()

    def flush(self):
        return self.connection.flush()


NoValue = object()
add_engine('mysql', MysqlEngine)


class MysqlTable(Table):
    def __init__(self, engine, table):
        super(MysqlTable, self).__init__(engine, table, param='%s')

    def add_column(self, name, type, not_null=False, default=NoValue, exist_ok=False, primary=False, auto_increment=False, collate=None):
        validate_name(name)
        assert re.match(r'^[\w\d\(\)]+$', type), 'Wrong type: {}'.format(type)
        values = []
        scolumn = '`{}` {}'.format(name, type)

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

        if self.table in self.engine.get_tables():
            if exist_ok:
                if self.get_column(name):
                    return
            if primary:
                scolumn += ', ADD PRIMARY KEY (`{}`)'.format(name)
            sql = 'ALTER TABLE `{}` ADD COLUMN {}'.format(self.table, scolumn)
        else:
            if primary:
                scolumn += ', PRIMARY KEY (`{}`)'.format(name)
            collate = collate or 'utf8mb4_unicode_ci'
            charset = collate.split('_')[0]
            sql = 'CREATE TABLE `{}` ({}) ENGINE=InnoDB DEFAULT CHARSET {} COLLATE {}'.format(self.table, scolumn, charset, collate)

        self.engine.cursor.execute(sql, tuple(values))

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

        sql = 'ALTER TABLE `{}` ADD {}{}({})'.format(self.table, index_type, name, column)
        self.engine.cursor.execute(sql)

    def has_index(self, name):
        self.engine.cursor.execute('show index from ' + self.table)
        for row in self.engine.cursor:
            if row[2] == name:
                return True
        return False
