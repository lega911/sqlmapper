
from sqlmapper import Connection


def run_mysql():
    run(Connection(host='127.0.0.1', user='root', db='example', autocreate=True, read_commited=True))


def run_psql():
    run(Connection(engine='postgresql', host='127.0.0.1', user='postgres', password='secret', db='example', autocreate=True))


def run_sqlite():
    run(Connection(engine='sqlite'))


def run(db):
    db.book.add_column('id', 'int', primary=True, auto_increment=True, exist_ok=True)
    db.book.add_column('name', 'text', exist_ok=True)
    db.book.add_column('value', 'int', exist_ok=True)

    db.book.insert({'name': 'ubuntu', 'value': 16})
    db.commit()
    d = db.book.find_one(1)
    print(d)

    db.book.update(1, {'value': 18})
    db.commit()

    for d in db.book.find({'name': 'ubuntu'}):
        print(d)
    
    db.book.delete({'value': 18})
    db.commit()

    print(db.book.count())


if __name__ == '__main__':
    run_psql()
    run_mysql()
    run_sqlite()
