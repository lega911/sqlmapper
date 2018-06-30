
from __future__ import absolute_import
import MySQLdb
import threading
import re
import copy
from .table import Table
from .utils import NoValue, validate_name
from .base_engine import BaseEngine


class Engine(BaseEngine):
    def __init__(self, autocreate=None, read_commited=False, **kw):
        self.read_commited = read_commited
        self.local = threading.local()
        if 'charset' not in kw:
            kw['charset'] = 'utf8mb4'
        super(Engine, self).__init__()

        self.db_config = {}
        for k in ['host', 'port', 'user', 'password', 'db', 'charset']:
            if k in kw:
                self.db_config[k] = kw[k]

        self.local.conn = self.get_connection(autocreate_db=autocreate)

    def get_connection(self, autocreate_db=False):
        try:
            return MySQLdb.connect(**self.db_config)
        except MySQLdb.OperationalError as e:
            if autocreate_db and e.args[0] == 1049:
                config = self.db_config.copy()
                db = config.pop('db')

                conn = MySQLdb.connect(**config)
                cursor = conn.cursor()
                cursor.execute('CREATE DATABASE {}'.format(db))
                conn.close()
                return MySQLdb.connect(**self.db_config)
            else:
                raise

    def commit(self):
        self.local.conn.commit()
        self.fire_event(True)
    
    def rollback(self):
        self.local.conn.rollback()
        self.fire_event(False)

    def close(self):
        self.local.conn.close()
        self.local.cursor = None
        self.local.conn = None

    def get_cursor(self):
        self.thread_init()
        if hasattr(self.local, 'cursor'):
            return self.local.cursor
        
        if not hasattr(self.local, 'conn'):
            self.local.conn = self.get_connection()

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
        self.thread_init()
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
