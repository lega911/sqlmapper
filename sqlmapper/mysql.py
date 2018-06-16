# coding: utf8

import MySQLdb
import threading
import re
import copy
from .table import Table
from .utils import NoValue, validate_name


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
        return MysqlTable(name, self)
    
    def get_tables(self):
        cursor = self.get_cursor()

        cursor.execute('SHOW TABLES')
        for row in cursor:
            yield row[0]

    def get_columns(self, table):
        result = self.local.tables.get(table)
        if not result:
            result = []
            cursor = self.get_cursor()
            cursor.execute('describe `{}`'.format(table))
            for row in cursor:
                result.append({
                    'name': row[0],
                    'type': row[1],
                    'null': row[2] == 'YES',
                    'default': row[4],
                    'primary': row[3] == 'PRI',
                    'auto_increment': row[5] == 'auto_increment'
                })
            self.local.tables[table] = result
        return copy.deepcopy(result)


class MysqlTable(Table):
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
