
import copy
import re
import aiomysql
from pymysql.err import InternalError, OperationalError
from ..utils import validate_name, NoValue, quote_key, format_func


class Engine:
    def __init__(self):
        self.cursors = []
        self.local = type('local', (object,), {'tables': {}})()

    async def init(self, *, loop, read_commited=False, autocreate=False, **kw):
        self.loop = loop
        self.read_commited = read_commited

        self._option = option = {}
        for k in ['db', 'host', 'port', 'user', 'password', 'charset']:
            if k in kw:
                option[k] = kw[k]

        if 'charset' not in option:
            option['charset'] = 'utf8mb4'

        try:
            self.connection = await aiomysql.connect(loop=loop, **option)
        except OperationalError as e:
            if autocreate and e.args[0] == 2003 and isinstance(e.__cause__, InternalError) and e.__cause__.args[0] == 1049:
                # Unknown database
                connect_opt = option.copy()
                db = connect_opt.pop('db')
                connection = await aiomysql.connect(loop=loop, **connect_opt)
                cursor = await connection.cursor()
                try:
                    await cursor.execute("CREATE DATABASE `{}`".format(db))
                finally:
                    await cursor.close()
                    connection.close()

                self.connection = await aiomysql.connect(loop=loop, **option)
            else:
                raise

    async def commit(self):
        await self.connection.commit()

    async def rollback(self):
        await self.connection.rollback()

    async def acquare_cursor(self):
        if self.cursors:
            return self.cursors.pop()

        cursor = await self.connection.cursor()
        if self.read_commited:
            await cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        return cursor

    def release_cursor(self, cursor):
        if not cursor.closed:
            self.cursors.append(cursor)

    async def reconnect(self):
        for cursor in self.cursors:
            await cursor.close()
        self.cursors = []

        if self.connection:
            self.connection.close()

        self.connection = await aiomysql.connect(loop=self.loop, **self._option)

    @property
    def cursor(self):
        return CursorContext(self)

    def try_execute(self, query, argv=None):
        return TryExecuteContext(self, query, argv)

    def get_table(self, name):
        return Table(name, self)

    async def get_tables(self):
        result = []
        async with self.try_execute('SHOW TABLES') as cursor:
            for row in await cursor.fetchall():
                result.append(row[0])
        return result

    async def get_columns(self, table):
        result = self.local.tables.get(table)
        if not result:
            result = []
            async with self.try_execute('describe `{}`'.format(table)) as cursor:
                for row in await cursor.fetchall():
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


class CursorContext:
    def __init__(self, engine):
        self.engine = engine

    async def __aenter__(self):
        self.cursor = await self.engine.acquare_cursor()
        return self.cursor

    async def __aexit__(self, exc_type, exc, tb):
        self.engine.release_cursor(self.cursor)


class TryExecuteContext:
    def __init__(self, engine, query, argv):
        self.engine = engine
        self.query = query
        self.argv = argv

    async def run(self):
        cursor = await self.engine.acquare_cursor()
        try:
            await cursor.execute(self.query, self.argv)
        except OperationalError as e:
            self.engine.release_cursor(cursor)
            if e.args[0] == 2013:
                await self.engine.reconnect()
                cursor = await self.engine.acquare_cursor()
                try:
                    await cursor.execute(self.query, self.argv)
                except Exception:
                    self.engine.release_cursor(cursor)
                    raise
            else:
                raise

        return cursor

    async def __aenter__(self):
        self.cursor = await self.run()
        return self.cursor

    async def __aexit__(self, exc_type, exc, tb):
        self.engine.release_cursor(self.cursor)

    async def __call__(self):
        self.engine.release_cursor(await self.run())


class Table:
    def __init__(self, name, engine):
        self.tablename = name
        self.engine = engine
        self.keyword = '%s'

    @property
    def cursor(self):
        return self.engine.cursor

    def try_execute(self, query, argv=None):
        return self.engine.try_execute(query, argv)

    async def describe(self):
        return await self.engine.get_columns(self.tablename)

    async def get_column(self, name):
        for column in await self.describe():
            if column['name'] == name:
                return column

    async def add_column(self, name, column_type, not_null=False, default=NoValue, exist_ok=False, primary=False, auto_increment=False, collate=None):
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

        if self.tablename in await self.engine.get_tables():
            if exist_ok:
                if await self.get_column(name):
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

        await self.try_execute(sql, tuple(values))()
        self.engine.local.tables[self.tablename] = None

    async def insert(self, data):
        keys = []
        values = []
        items = []
        for key, value in data.items():
            keys.append(quote_key(key))
            values.append(value)
            items.append(self.keyword)

        sql = 'INSERT INTO `{}` ({}) VALUES ({})'.format(self.tablename, ', '.join(keys), ', '.join(items))
        async with self.try_execute(sql, tuple(values)) as cursor:
            assert cursor.rowcount == 1
            return cursor.lastrowid

    async def _build_filter(self, filter):
        if filter is None:
            return None, []
        elif isinstance(filter, dict):
            keys = []
            values = []
            for k, v in filter.items():
                if '.' not in k:
                    k = self.tablename +  '.' + k
                k = quote_key(k)
                if v is None:
                    keys.append(k + ' is NULL')
                else:
                    keys.append(k + '=' + self.keyword)
                    values.append(v)
            sql = ' AND '.join(keys)
            return sql, values
        elif isinstance(filter, (list, tuple)):
            return filter[0], filter[1:]
        elif isinstance(filter, (int, str, bytes)):
            # find by primary key
            key = None
            for column in await self.describe():
                if column['primary']:
                    key = column['name']
                    break
            else:
                raise ValueError('No primary key')
            return '`{}` = {}'.format(key, self.keyword), [filter]
        else:
            raise NotImplementedError

    async def find_one(self, filter=None, join=None, left_join=None, for_update=False, columns=None, order_by=None):
        result = list(await self.find(filter, limit=1, join=join, for_update=for_update, columns=columns, order_by=order_by))
        if result:
            return result[0]

    async def find(self, filter=None, limit=None, join=None, left_join=None, for_update=False, columns=None, group_by=None, order_by=None):
        """
            join='subtable.id=column'
            join='subtable as tbl.id=column'
        """

        if columns:
            assert not join
            if not isinstance(columns, (list, tuple)):
                columns = [columns]
            columns = ', '.join(map(format_func, columns))
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

            columns += ', "" as __divider, {}.*'.format(alias)
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

        sql = 'SELECT {} FROM `{}`'.format(columns, self.tablename)
        where, values = await self._build_filter(filter)
        if join:
            sql += join
        if where:
            sql += ' WHERE ' + where
        if group_by:
            sql += ' GROUP BY ' + quote_key(group_by)
        if order_by:
            if not isinstance(order_by, list):
                order_by = [order_by]
            oc = []
            for name in order_by:
                if name.startswith('-'):
                    oc.append(quote_key(name[1:]) + ' DESC')
                else:
                    oc.append(quote_key(name))
            sql += ' ORDER BY ' + ', '.join(oc)
        if limit:
            assert isinstance(limit, int)
            sql += ' LIMIT {}'.format(limit)

        if for_update:
            sql += ' FOR UPDATE'

        result = []
        async with self.try_execute(sql, tuple(values)) as cursor:
            if cursor.rowcount:
                columns = cursor.description
                for row in await cursor.fetchall():
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
                    result.append(d)

        return result

    async def update(self, filter=None, update=None, limit=None):
        up = []
        values = []
        for key, value in update.items():
            up.append('`{}` = {}'.format(key, self.keyword))
            values.append(value)

        sql = 'UPDATE `{}` SET {}'.format(self.tablename, ', '.join(up))

        where, wvalues = await self._build_filter(filter)
        if where:
            sql += ' WHERE ' + where
            values += wvalues

        if limit:
            assert isinstance(limit, int)
            sql += ' LIMIT {}'.format(limit)

        await self.try_execute(sql, tuple(values))()

    async def update_one(self, filter=None, update=None):
        await self.update(filter, update, limit=1)

    async def delete(self, filter=None):
        where, values = await self._build_filter(filter)

        sql = 'DELETE FROM `{}`'.format(self.tablename)
        if where:
            sql += ' WHERE {}'.format(where)
        await self.try_execute(sql, tuple(values))()

    async def create_index(self, name, column, primary=False, unique=False, fulltext=False, exist_ok=False):
        if primary:
            name = 'PRIMARY'
        if exist_ok and await self.has_index(name):
            return

        if isinstance(column, list):
            column = ', '.join(map(quote_key, column))
        else:
            column = quote_key(column)

        index_type = 'INDEX '
        if primary:
            index_type = 'PRIMARY KEY '
            assert not fulltext
            name = ''
        else:
            name = quote_key(name)

        if unique and not primary:
            assert not fulltext
            index_type = 'UNIQUE '
        elif fulltext:
            index_type = 'FULLTEXT '

        sql = 'ALTER TABLE `{}` ADD {}{}({})'.format(self.tablename, index_type, name, column)
        await self.try_execute(sql)()

    async def has_index(self, name):
        async with self.try_execute('show index from ' + self.tablename) as cursor:
            for row in await cursor.fetchall():
                if row[2] == name:
                    return True
            return False

    async def count(self, filter=None):
        where, values = await self._build_filter(filter)

        sql = 'SELECT COUNT(*) FROM `{}`'.format(self.tablename)
        if where:
            sql += ' WHERE {}'.format(where)
        async with self.try_execute(sql, tuple(values)) as cursor:
            return (await cursor.fetchone())[0]

    async def drop(self, exist_ok=True):
        sql = 'DROP TABLE '
        if exist_ok:
            sql += 'IF EXISTS '
        sql += self.tablename
        await self.try_execute(sql)()
