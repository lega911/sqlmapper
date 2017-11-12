# Sqlmapper
Wrapper for SQL

# Example
```python
connection = Connection(db='example', charset='utf8')

with connection() as db:
    # add columns into table `tblname` if not exists
    db.tblname.add_column('name', 'VARCHAR(32)', exist_ok=True)

    # insert row into `tblname`
    db.tblname.insert({'name': 'Ubuntu', 'value': 10, 'comment': 'Linux'})

    # update
    db.tblname.update({'name': 'Ubuntu'}, {'value': 16})

    # get one row where primary key == 1
    d = db.tblname.find_one(1)

    for d in db.tblname.find({'name': 'Ubuntu'}):
        print(d)
```
