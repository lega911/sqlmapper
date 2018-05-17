# Sqlmapper
Easy wrapper for SQL

* Supports Python 2.x, 3.x, MySQL (PostgreSQL is coming)
* License [MIT](http://opensource.org/licenses/MIT)

### Install and update using pip

```bash
pip install -U sqlmapper
```

### Examples
```python
connection = Connection(db='example')

with connection() as db:
    db.tblname.insert({'name': 'Ubuntu', 'value': 14})
    # INSERT INTO `tblname` (`name`, `value`) VALUES ('Ubuntu', 14)

    db.tblname.insert({'name': 'MacOS', 'value': 10})
    # INSERT INTO `tblname` (`name`, `value`) VALUES ('MacOS', 10)

    for d in db.tblname.find({'name': 'Ubuntu'}):
        # SELECT tblname.* FROM `tblname` WHERE `tblname`.`name`='Ubuntu'
        print(d)

    db.tblname.update({'name': 'Ubuntu'}, {'value': 16})
    # UPDATE `tblname` SET `value` = 16 WHERE `tblname`.`name`='Ubuntu'

    db.tblname.find_one({'Name': 'Ubuntu'})
    # SELECT tblname.* FROM `tblname` WHERE `name` = 'Ubuntu' LIMIT 1

    db.tblname.find_one(2)
    # SELECT tblname.* FROM `tblname` WHERE `id` = 2 LIMIT 1

    db.tblname.delete({'name': 'MacOS'})
    # DELETE FROM `tblname` WHERE `tblname`.`name`='MacOS'
# commit
```

### Change schema
```python

# a table is created for first column
db.tblname.add_column('id', 'INT(11)', primary=True, auto_increment=True, exist_ok=True)
# CREATE TABLE `tblname` (`id` INT(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci

db.tblname.add_column('name', 'VARCHAR(32)', exist_ok=True)
# ALTER TABLE `tblname` ADD COLUMN `name` VARCHAR(32)

db.tblname.add_column('value', 'INT(11)', exist_ok=True)
# ALTER TABLE `tblname` ADD COLUMN `value` INT(11)

db.tblname.create_index('name_idx', ['name'], exist_ok=True)
# ALTER TABLE `tblname` ADD INDEX `name_idx`(`name`)
```

### Join
```python
for d in db.parent.find({'name': 'Linux'}, join='child.id=child_id'):
    # SELECT parent.*, "" as __divider, child.* FROM `parent` JOIN child AS child ON child.id = child_id WHERE `parent`.`name`='Linux'
    print(d)
    # d == {
    #   'name': 'Linux',
    #   'child_id': 5,
    #   'child': {
    #       'id': 5,
    #       'name': 'Ubuntu'
    #   }
    # }
```

### Group by
```python
for d in db.tblname.find(group_by='name', columns=['name', 'SUM(value)']):
    # SELECT `name`, SUM(`value`) as `sum_value` FROM `tblname` GROUP BY `name`
    print(d)  # {'name': u'Ubuntu', 'sum_value': 32}
```
