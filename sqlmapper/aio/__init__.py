
import asyncio

async def Connection(**kw):
    engine = kw.pop('engine', None) or 'mysql'
    loop = kw.pop('engine', None) or asyncio.get_event_loop()

    if engine == 'mysql':
        from .amysql import Engine
        engine = Engine()
        await engine.init(loop=loop, **kw)
        return AsyncConnection(engine)
    else:
        raise NotImplementedError()


class AsyncConnection:
    def __init__(self, engine):
        self._engine = engine

    async def commit(self):
        await self._engine.commit()
    
    async def rollback(self):
        await self._engine.rollback()

    def close(self):
        self._engine.close()
    
    def __getitem__(self, name):
        return self._engine.get_table(name)

    def __getattr__(self, name):
        return self[name]

    def __aiter__(self):
        return DBList(self._engine)

    def on_commit(self, fn):
        self._engine.on_commit(fn)
    
    def on_rollback(self, fn):
        self._engine.on_rollback(fn)
    
    @property
    def cursor(self):
        return self._engine.cursor

    '''
    def __enter__(self):
        self._engine.local.contextlvl += 1

    def __exit__(self, exc_type, exc_value, traceback):
        self._engine.local.contextlvl -= 1
        if not self._engine.local.contextlvl:
            if exc_type:
                self.rollback()
            else:
                self.commit()
        if exc_type:
            return False
    '''

class DBList:
    def __init__(self, engine):
        self.engine = engine
        self.result = None
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.result is None:
            self.result = await self.engine.get_tables()

        if self.index >= len(self.result):
            raise StopAsyncIteration

        value = self.result[self.index]
        self.index += 1
        return value
