
class MultiException(Exception):
    def __init__(self, e):
        super(MultiException, self).__init__()
        self.exceptions = e


class BaseEngine(object):
    def __init__(self):
        if not hasattr(self, 'local'):
            self.local = type('local', (object,), {})()
        self.thread_init()

    def thread_init(self):
        if hasattr(self.local, 'tables'):
            return
        self.local.tables = {}
        self.local.commit = []
        self.local.rollback = []

    def fire_event(self, success):
        self.thread_init()
        fnlist = self.local.commit if success else self.local.rollback
        self.local.commit = []
        self.local.rollback = []
        exceptions = []
        for fn in fnlist:
            try:
                fn()
            except Exception as e:
                exceptions.append(e)
        if exceptions:
            if len(exceptions) == 1:
                raise exceptions[0]
            else:
                raise MultiException(exceptions)

    def on_commit(self, fn):
        self.thread_init()
        self.local.commit.append(fn)

    def on_rollback(self, fn):
        self.thread_init()
        self.local.rollback.append(fn)
