
from __future__ import absolute_import
import psycopg2
import threading
import re
import copy
from .table import Table
from .utils import NoValue, validate_name
from .base_engine import BaseEngine


class Engine(BaseEngine):
    def __init__(self, schema='public', autocreate=None, read_commited=False, **kw):
        self.read_commited = read_commited
        self.local = threading.local()
        self.schema = schema
        super(Engine, self).__init__()

        self.db_config = {}
        for k in ['host', 'port', 'user', 'password', 'dbname']:
            if k in kw:
                self.db_config[k] = kw[k]
        if not self.db_config.get('dbname'):
            self.db_config['dbname'] = kw.get('db')

        self.local.conn = self.get_connection(autocreate_db=autocreate)

    def get_connection(self, autocreate_db=False):
        try:
            return psycopg2.connect(**self.db_config)
        except psycopg2.OperationalError as e:
            if autocreate_db and 'does not exist' in str(e):
                config = self.db_config.copy()
                db = config.pop('dbname')

                conn = psycopg2.connect(**config)
                conn.autocommit = True
                cursor = conn.cursor()
                cursor.execute('CREATE DATABASE {}'.format(db))
                conn.close()
                return psycopg2.connect(**self.db_config)
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
        cursor.execute('SET search_path TO ' + self.schema)
        return cursor

    def get_table(self, name):
        return PsqlTable(name, self)
    
    def get_tables(self):
        cursor = self.get_cursor()
        cursor.execute('SELECT tablename FROM pg_catalog.pg_tables where schemaname=%s', (self.schema,))
        for row in cursor:
            yield row[0]

    def get_columns(self, table):
        self.thread_init()
        result = self.local.tables.get(table)
        if not result:
            result = []
            cursor = self.get_cursor()
            # get primary key

            primary = set()
            cursor.execute(
                'SELECT c.column_name, c.data_type FROM '
                'information_schema.table_constraints tc '
                'JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) '
                'JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema AND tc.table_name = c.table_name AND ccu.column_name = c.column_name '
                'where constraint_type = %s and tc.table_name = %s', ('PRIMARY KEY', table))
            for row in cursor:
                primary.add(row[0])

            cursor.execute('select column_name, is_nullable, data_type, column_default, numeric_precision, numeric_precision_radix, * from INFORMATION_SCHEMA.COLUMNS where table_catalog=%s and table_schema=%s and table_name=%s', (self.db_config['dbname'], self.schema, table))
            for row in cursor:
                result.append({
                    'name': row[0],
                    'null': row[1] == 'YES',
                    'type': row[2],
                    'default': row[3],
                    'primary': row[0] in primary
                    #'auto_increment': False
                })
            self.local.tables[table] = result
        return copy.deepcopy(result)


class PsqlTable(Table):
    def __init__(self, *a, **kw):
        super(PsqlTable, self).__init__(*a, quote='"', **kw)

    def add_column(self, name, column_type, not_null=False, default=NoValue, exist_ok=False, primary=False, auto_increment=False, collate=None):
        validate_name(name)
        assert re.match(r'^[\w\d\(\)]+$', column_type), 'Wrong type: {}'.format(column_type)
        values = []

        if primary:
            if auto_increment:
                if column_type == 'bigint':
                    column_type = 'bigserial'
                else:
                    column_type = 'serial'

        scolumn = '{} {}'.format(name, column_type)

        if primary:
            scolumn += ' PRIMARY KEY'

        if not_null:
            scolumn += ' NOT NULL'

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
                raise NotImplementedError()
            sql = 'ALTER TABLE {} ADD COLUMN {}'.format(self.tablename, scolumn)
        else:
            sql = 'CREATE TABLE {} ({})'.format(self.tablename, scolumn)

        self.cursor.execute(sql, tuple(values))
        self.engine.commit()
        self.engine.local.tables[self.tablename] = None

    def has_index(self, name):
        self.cursor.execute('select i.relname '
            'from pg_class t, pg_class i, pg_index ix, pg_attribute a '
            'where t.oid = ix.indrelid and i.oid = ix.indexrelid '
            'and a.attrelid = t.oid and a.attnum = ANY(ix.indkey) '
            'and t.relkind = %s and t.relname = %s', ('r', self.tablename))
        for row in self.cursor:
            if row[0] == name:
                return True
        return False

    def create_index(self, name, column, unique=False, exist_ok=False):
        if exist_ok and self.has_index(name):
            return

        if isinstance(column, list):
            column = ', '.join(map(self.cc, column))
        else:
            column = self.cc(column)

        if unique:
            index_type = 'UNIQUE INDEX'
        else:
            index_type = 'INDEX'

        sql = 'CREATE {} {} ON {} ({})'.format(index_type, self.cc(name), self.cc(self.tablename), column)
        self.cursor.execute(sql)
