# coding: utf8

from __future__ import absolute_import


class Connection(object):
    def __init__(self, **kw):
        engine = kw.pop('engine', None) or 'mysql'

        if engine == 'mysql':
            from .mysql import Engine as engine
        elif engine == 'sqlite':
            from .sqlite import Engine as engine
        elif not callable(engine):
            raise NotImplementedError
        self._engine = engine(**kw)

    def commit(self):
        self._engine.commit()
    
    def rollback(self):
        self._engine.rollback()

    def close(self):
        self._engine.close()
    
    def __getitem__(self, name):
        return self._engine.get_table(name)

    def __getattr__(self, name):
        return self[name]

    def __iter__(self):
        return self._engine.get_tables()

    def on_commit(self, fn):
        self._engine.on_commit(fn)
    
    def on_rollback(self, fn):
        self._engine.on_rollback(fn)
