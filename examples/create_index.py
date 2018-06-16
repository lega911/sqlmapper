
from sqlmapper import Connection


def main():
    db = Connection(host='127.0.0.1', user='root', db='example', autocreate=True)

    db.book.add_column('id', 'int', primary=True, auto_increment=True, exist_ok=True)
    db.book.add_column('name', 'varchar(32)', exist_ok=True)

    db.book.create_index('nameindex', 'name', unique=True, exist_ok=True)

    db.book.insert({'name': 'ubuntu'})
    db.commit()


if __name__ == '__main__':
    main()
