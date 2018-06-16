
from sqlmapper import Connection


def main():
    # docker run -d -p 3306:3306 --name mysql56 -e MYSQL_ALLOW_EMPTY_PASSWORD=true -d mysql:5.6
    db = Connection(host='127.0.0.1', user='root', db='example', autocreate=True, read_commited=True)

    db.book.add_column('id', 'int', primary=True, auto_increment=True, exist_ok=True)
    db.book.add_column('name', 'text', exist_ok=True)
    db.book.add_column('value', 'int', exist_ok=True)

    db.book.insert({'name': 'ubuntu', 'value': 16})
    db.commit()
    d = db.book.find_one(2)
    print(d)

    for d in db.book.find({'name': 'ubuntu'}):
        print(d)


if __name__ == '__main__':
    main()
