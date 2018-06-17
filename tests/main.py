
import unittest
from sqlmapper import Connection


class TestMySQL(unittest.TestCase):
    def test_mysql(self):
        self.main(Connection(host='127.0.0.1', db='unittest', user='root', autocreate=True, read_commited=True))
    
    def test_sqlite(self):
        self.main(Connection(engine='sqlite'))

    def main(self, db):
        db.book.drop()
        db.ref.drop()

        db.book.add_column('id', 'int', primary=True, auto_increment=True, exist_ok=True)
        db.book.add_column('name', 'text', exist_ok=True)
        db.book.add_column('value', 'int', exist_ok=True)
        self.assertEqual(db['book'].count(), 0)

        self.assertEqual(len(db.book.describe()), 3)
        self.assertEqual(db['book'].get_column('value')['name'], 'value')
        self.assertIn('book', db)

        db.book.insert({'name': 'ubuntu', 'value': 16})
        db.book.insert({'name': 'mint', 'value': 18})
        db.book.insert({'name': 'debian', 'value': 9})
        db.book.insert({'name': 'debian', 'value': 8})
        db.book.insert({'name': 'redhat', 'value': 0})
        db.book.insert({'name': 'macos', 'value': 10})
        db.book.insert({'name': 'ubuntu', 'value': 18})
        db.book.insert({'name': 'ubuntu', 'value': 14})
        db.commit()

        self.assertEqual(db.book.count(), 8)

        @db.on_commit
        def on_commit():
            self.status = 1

        @db.on_rollback
        def on_rollback():
            self.status = 2

        db.book.update({'name': 'redhat'}, {'value': 5})
        db.commit()

        self.assertEqual(self.status, 1)

        @db.on_commit
        def on_commit():
            self.status = 3

        @db.on_rollback
        def on_rollback():
            self.status = 4

        db.book.update({'name': 'redhat'}, {'value': 25})
        db.rollback()
        self.assertEqual(self.status, 4)

        for d in db.book.find({'name': 'redhat'}):
            self.status += 1
            self.assertEqual(d['value'], 5)
        
        self.assertEqual(self.status, 5)

        self.assertEqual(db.book.find_one(3)['value'], 9)

        db.book.delete({'name': 'macos'})
        self.assertEqual(db.book.count(), 7)
        db.commit()

        r = list(db.book.find(group_by='name', columns=['name', 'COUNT(value)'], order_by='-count_value'))
        self.assertEqual(len(r), 4)
        self.assertEqual(r[0]['name'], 'ubuntu')
        self.assertEqual(r[0]['count_value'], 3)
        self.assertEqual(r[1]['name'], 'debian')
        self.assertEqual(r[1]['count_value'], 2)

        db.book.add_column('ext', 'int', exist_ok=True)
        self.assertEqual(len(db.book.describe()), 4)

        self.assertFalse(db.book.has_index('ext_index'))
        db.book.create_index('ext_index', 'ext', unique=True, exist_ok=True)
        self.assertTrue(db.book.has_index('ext_index'))

        db.book.update(1, {'ext': 10})
        db.book.update(2, {'ext': 20})
        db.book.update(3, {'ext': 30})
        db.commit()

        with self.assertRaises(Exception):
            db.book.update(4, {'ext': 10})
            db.commit()
        
        db.ref.add_column('id', 'int', primary=True, auto_increment=True)
        db.ref.add_column('book_id', 'int')
        db.ref.insert({'book_id': 1})
        db.ref.insert({'book_id': 2})
        db.ref.insert({'book_id': 3})
        db.ref.insert({'book_id': 6})
        db.ref.insert({'book_id': 1})

        r = list(db.ref.find(join='book.id=book_id', order_by='ref.id'))
        self.assertEqual(len(r), 4)
        self.assertEqual(r[1]['book']['value'], 18)
        self.assertEqual(r[2]['book']['value'], 9)

        r = list(db.ref.find(left_join='book.id=book_id', order_by='ref.id'))
        self.assertEqual(len(r), 5)
        self.assertIsNone(r[3]['book'], None)
        db.close()

    def test_context(self):
        pass
    
    def test_threading(self):
        pass


if __name__ == '__main__':
    unittest.main()
