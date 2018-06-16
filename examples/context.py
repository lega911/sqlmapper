
from sqlmapper import Connection


def main():
    db = Connection(host='127.0.0.1', user='root', db='example', autocreate=True, read_commited=True)

    def add_row(name):
        with db:
            print('insert', name)
            db.book.insert({'name': name})

            @db.on_commit
            def msg():
                print('commit', name)

    add_row('RedHat')
    print()

    with db:
        add_row('Linux')
        add_row('Ubuntu')
        add_row('Debian')

        print('* group commit')


if __name__ == '__main__':
    main()
