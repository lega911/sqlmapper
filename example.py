
from sqlmapper import Connection


def main():
    connection = Connection(db='example', charset='utf8')

    with connection() as db:
        # add columns into table `tblname`
        db.tblname.add_column('id', 'INT(11)', primary=True, auto_increment=True, exist_ok=True)
        db.tblname.add_column('name', 'VARCHAR(32)', exist_ok=True)
        db.tblname.add_column('value', 'INT(11)', exist_ok=True)
        db.tblname.add_column('comment', 'TEXT', exist_ok=True)

    with connection() as db:
        # insert row into `tblname`
        db.tblname.insert({'name': 'Ubuntu', 'value': 10, 'comment': 'Linux'})

        for d in db.tblname.find({'name': 'Ubuntu'}):
            print(d)

        db.tblname.update({'name': 'Ubuntu'}, {'value': 16})

        # get one row where primary key == 1
        d = db.tblname.find_one(1)
        print(d)

        # more complex filter
        d = db.tblname.find_one(['value < %s and name = %s', 20, 'Ubuntu'])
        print(d)


if __name__ == '__main__':
    main()
