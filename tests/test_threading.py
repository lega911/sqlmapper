
import time
import threading
from sqlmapper import Connection


def get_db():
    db = Connection(host='127.0.0.1', db='unittest', user='root', autocreate=True, read_commited=True)
    db.book.drop()

    db.book.add_column('id', 'int', primary=True, auto_increment=True, exist_ok=True)
    db.book.add_column('value', 'int', exist_ok=True)

    db.book.insert({'value': 5})
    db.commit()
    return db


def test_threading0():
    result = []
    def add(*a):
        result.append('.'.join(map(str, a)))

    db = get_db()
    db.book.update(1, {'value': 20})

    def run():
        d = db.book.find_one(1)
        add('t', d['value'])

        d = db.book.find_one(1, for_update=True)
        add('t', d['value'])
        db.book.update(1, {'value': d['value'] + 10})
        time.sleep(0.2)
        add('t.commit')
        db.commit()

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.5)
    add('m.commit')
    db.commit()

    time.sleep(0.1)
    d = db.book.find_one(1)
    add('m', d['value'])

    time.sleep(0.2)
    d = db.book.find_one(1)
    add('m', d['value'])

    t.join()
    assert result == ['t.5', 'm.commit', 't.20', 'm.20', 't.commit', 'm.30']


def test_threading1():
    result = []
    def add(*a):
        result.append('.'.join(map(str, a)))

    db = get_db()

    def run(n):
        for i in range(5):
            d = db.book.find_one(1, for_update=True)
            add(d['value'])
            db.book.update(1, {'value': d['value'] + 1})
            db.commit()

    threads = []
    for i in range(5):
        t = threading.Thread(target=run, args=(i,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    
    assert db.book.find_one(1)['value'] == 30
    assert result == ['5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29']
