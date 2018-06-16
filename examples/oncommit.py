
from sqlmapper import Connection


def main():
    db = Connection(host='127.0.0.1', user='root', db='example', autocreate=True, read_commited=True)

    @db.on_commit
    def after_commit():
        print('commited')
    
    @db.on_rollback
    def after_rollback():
        print('rollback')

    db.book.insert({'name': 'ubuntu', 'value': 3})

    print('start commit')
    db.commit()


if __name__ == '__main__':
    main()
