# coding: utf8

from __future__ import absolute_import
from . import utils
from . import esqlite
from . import emysql


class Connection(object):
    def __init__(self, engine='mysql', **option):
        if not callable(engine):
            engine = utils.engine_list.get(engine)
            if not engine:
                raise Exception('Not supported engine')

        self.__engine__ = engine(option)

    def commit(self):
        return self.__engine__.commit()

    def rollback(self):
        return self.__engine__.rollback()

    def flush(self):
        return self.__engine__.flush()

    def __getitem__(self, name):
        return self.__engine__.table(name)

    def __getattr__(self, name):
        return self[name]

    def get_tables(self):
        return self.__engine__.get_tables()

    def __iter__(self):
        return iter(self.__engine__.get_tables())
