# coding: utf8

from __future__ import absolute_import
from .utils import validate_name, add_engine
from .table import Table


class SqliteEngine(object):
    def __init__(self, option):
        import sqlite3
        self.connection = sqlite3.connect(option.get('db') or ':memory:')
        self.cursor = self.connection.cursor()

    def table(self, name):
        return SqliteTable(self, name)

    def get_tables(self):
        self.cursor.execute('SELECT name FROM sqlite_master WHERE type = ?', ('table',))
        for row in self.cursor:
            yield row[0]

    def describe(self, table):
        result = []
        cursor = self.cursor
        cursor.execute('PRAGMA table_info({})'.format(table))
        for row in cursor:
            result.append({
                'name': row[1],
                'type': row[2],
                'notnull': row[3] == 1,
                'default': row[4],
                'primary': row[5] == 1
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
        pass


add_engine('sqlite', SqliteEngine)
NoValue = object()
sqlite_types = {
    # integer
    'INTEGER': 'INTEGER',
    'INT': 'INTEGER',

    # text
    'TEXT': 'TEXT',
    'VARCHAR': 'TEXT',

    # none
    'NONE': 'NONE',
    'BLOB': 'NONE',

    # real
    'REAL': 'REAL',
    'DOUBLE': 'REAL',
    'FLOAT': 'REAL',

    # numeric
    'NUMERIC': 'NUMERIC',
    'DECIMAL': 'NUMERIC',
    'BOOLEAN': 'NUMERIC',
    'DATE': 'NUMERIC',
    'DATETIME': 'NUMERIC'
}


class SqliteTable(Table):
    def __init__(self, engine, table):
        super(SqliteTable, self).__init__(engine, table, param='?')

    def add_column(self, name, type, default=NoValue, exist_ok=False, primary=False, auto_increment=False, not_null=False):
        validate_name(name)

        type = sqlite_types.get(type.upper())
        if not type:
            raise ValueError('Wrong type')

        values = []
        scolumn = '`{}` {}'.format(name, type)

        if primary:
            scolumn += ' PRIMARY KEY'
            if auto_increment:
                scolumn += ' AUTOINCREMENT'
        elif not_null:
            scolumn += ' NOT NULL'

        if default != NoValue:
            if primary:
                raise ValueError('Can''t have default value')
            scolumn += ' DEFAULT ?'
            values.append(default)

        if self.table in self.engine.get_tables():
            if exist_ok:
                if self.engine.get_column(self.table, name):
                    return
            sql = 'ALTER TABLE `{}` ADD COLUMN {}'.format(self.table, scolumn)
        else:
            sql = 'CREATE TABLE {} ({})'.format(self.table, scolumn)

        self.engine.cursor.execute(sql, tuple(values))
