
import pytest
from sqlmapper import Connection


def test_mysql():
    main(Connection(host='127.0.0.1', db='unittest', user='root', autocreate=True, read_commited=True))


def test_psql():
    main(Connection(engine='postgresql', host='127.0.0.1', db='unittest', user='postgres', password='secret', autocreate=True))


def test_sqlite():
    main(Connection(engine='sqlite'))


def main(db):
    db.book.drop()
    db.ref.drop()

    db.book.add_column('id', 'int', primary=True, auto_increment=True, exist_ok=True)
    db.book.add_column('name', 'text', exist_ok=True)
    db.book.add_column('value', 'int', exist_ok=True)
    assert db['book'].count() == 0

    assert len(db.book.describe()) == 3
    assert db['book'].get_column('value')['name'] == 'value'
    assert 'book' in db

    db.book.insert({'name': 'ubuntu', 'value': 16})
    db.book.insert({'name': 'mint', 'value': 18})
    db.book.insert({'name': 'debian', 'value': 9})
    db.book.insert({'name': 'debian', 'value': 8})
    db.book.insert({'name': 'redhat', 'value': 0})
    db.book.insert({'name': 'macos', 'value': 10})
    db.book.insert({'name': 'ubuntu', 'value': 18})
    db.book.insert({'name': 'ubuntu', 'value': 14})
    db.commit()

    assert db.book.count() == 8

    class dd:
        status = 0

    @db.on_commit
    def on_commit():
        dd.status = 1

    @db.on_rollback
    def on_rollback():
        dd.status = 2

    db.book.update({'name': 'redhat'}, {'value': 5})
    db.commit()

    assert dd.status == 1

    @db.on_commit
    def on_commit():
        dd.status = 3

    @db.on_rollback
    def on_rollback():
        dd.status = 4

    db.book.update({'name': 'redhat'}, {'value': 25})
    db.rollback()
    assert dd.status == 4

    for d in db.book.find({'name': 'redhat'}):
        dd.status += 1
        assert d['value'] == 5
    
    assert dd.status == 5

    assert db.book.find_one(3)['value'] == 9

    db.book.delete({'name': 'macos'})
    assert db.book.count() == 7
    db.commit()

    r = list(db.book.find(group_by='name', columns=['name', 'COUNT(value)'], order_by='-count_value'))
    assert len(r) == 4
    assert r[0]['name'] == 'ubuntu'
    assert r[0]['count_value'] == 3
    assert r[1]['name'] == 'debian'
    assert r[1]['count_value'] == 2

    db.book.add_column('ext', 'int', exist_ok=True)
    assert len(db.book.describe()) == 4

    assert db.book.has_index('ext_index') is False
    db.book.create_index('ext_index', 'ext', unique=True, exist_ok=True)
    assert db.book.has_index('ext_index') is True

    db.book.update(1, {'ext': 10})
    db.book.update(2, {'ext': 20})
    db.book.update(3, {'ext': 30})
    db.commit()

    with pytest.raises(Exception):
        db.book.update(4, {'ext': 10})
        db.commit()

    db.rollback()
    
    db.ref.add_column('id', 'int', primary=True, auto_increment=True)
    db.ref.add_column('book_id', 'int')
    db.ref.insert({'book_id': 1})
    db.ref.insert({'book_id': 2})
    db.ref.insert({'book_id': 3})
    db.ref.insert({'book_id': 6})
    db.ref.insert({'book_id': 1})

    r = list(db.ref.find(join='book.id=book_id', order_by='ref.id'))
    assert len(r) == 4
    assert r[1]['book']['value'] == 18
    assert r[2]['book']['value'] == 9

    r = list(db.ref.find(left_join='book.id=book_id', order_by='ref.id'))
    assert len(r) == 5
    assert r[3]['book'] is None
    db.close()


def test_context():
    pass
