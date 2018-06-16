
from sqlmapper import Connection


def main():
    db = Connection(host='127.0.0.1', user='root', db='example', autocreate=True)

    for table in db:
        print(table)


if __name__ == '__main__':
    main()
