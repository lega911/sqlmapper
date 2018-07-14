
from __future__ import absolute_import
import sqlite3
import copy
from .table import Table
from .utils import validate_name, NoValue, quote_key
from .base_engine import BaseEngine


class Engine(BaseEngine):
    def __init__(self, **kw):
        self.conn = sqlite3.connect(kw.get('db') or ':memory:')
        self.cursor = None
        super(Engine, self).__init__()

    def get_cursor(self):
        if not self.cursor:
            self.cursor = self.conn.cursor()
        return self.cursor

    def commit(self):
        self.conn.commit()
        self.fire_event(True)
    
    def rollback(self):
        self.conn.rollback()
        self.fire_event(False)

    def close(self):
        self.conn.close()
        self.conn = None

    def get_columns(self, table):
        result = self.local.tables.get(table)
        if not result:
            result = []
            cursor = self.get_cursor()
            cursor.execute('PRAGMA table_info({})'.format(table))
            for row in cursor:
                result.append({
                    'name': row[1],
                    'type': row[2],
                    'notnull': row[3] == 1,
                    'default': row[4],
                    'primary': row[5] == 1
                })
            self.local.tables[table] = result
        return copy.deepcopy(result)

    def get_table(self, name):
        return SqliteTable(name, self, keyword='?')

    def get_tables(self):
        cursor = self.get_cursor()
        cursor.execute('SELECT name FROM sqlite_master WHERE type = ?', ('table',))
        for row in cursor:
            yield row[0]


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

        if self.tablename in self.engine.get_tables():
            if exist_ok:
                if self.get_column(name):
                    return
            sql = 'ALTER TABLE `{}` ADD COLUMN {}'.format(self.tablename, scolumn)
        else:
            sql = 'CREATE TABLE {} ({})'.format(self.tablename, scolumn)

        self.cursor.execute(sql, tuple(values))
        self.engine.local.tables[self.tablename] = None

    def has_index(self, name):
        self.cursor.execute('PRAGMA index_list({})'.format(self.tablename))
        for row in self.cursor:
            if row[1] == name:
                return True
        return False

    def create_index(self, name, column, unique=False, exist_ok=False):
        if exist_ok and self.has_index(name):
            return
        
        if not isinstance(column, list):
            column = [column]
        
        column = ', '.join(map(self.cc, column))
        sql = 'CREATE {}INDEX {} on {} ({})'.format(
            'UNIQUE ' if unique else '',
            name,
            self.tablename,
            column
        )
        self.cursor.execute(sql)

    def _build_filter(self, filter):
        s, v = super(SqliteTable, self)._build_filter(filter)
        if isinstance(filter, (list, tuple)) and s:
            s = s.replace('%s', '?')
        return s, v
