
from sqlmapper import Connection


def main():
    connection = Connection(db='example', charset='utf8')

    with connection() as db:
        db.aabook.add_column('id', 'INT(11)', primary=True, auto_increment=True, exist_ok=True)
        db.aabook.add_column('name', 'VARCHAR(32)', exist_ok=True)
        db.aabook.add_column('value', 'INT(11)', exist_ok=True)
        db.aabook.add_column('comment', 'TEXT', exist_ok=True)

    with connection() as db:
        db.aabook.insert({'name': 'Ubuntu', 'value': 10, 'comment': 'Linux'})

        for d in db.aabook.find({'name': 'Ubuntu'}):
            print(d)

        db.aabook.update({'name': 'Ubuntu'}, {'value': 16})

        d = db.aabook.find_one(1)  # get one when primary key == 1
        print(d)

        d = db.aabook.find_one(['value < %s and name = %s', 20, 'Ubuntu'])  # more complex filter
        print(d)


if __name__ == '__main__':
    main()
