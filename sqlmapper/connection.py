# coding: utf8

from __future__ import absolute_import


class Connection(object):
    def __init__(self, **kw):
        engine = kw.pop('engine', None) or 'mysql'

        if engine == 'mysql':
            from .mysql import Engine
            self._engine = Engine(**kw)
        elif engine == 'sqlite':
            raise NotImplementedError
        else:
            self._engine = engine()

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
