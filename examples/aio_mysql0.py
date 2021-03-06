
import asyncio
from sqlmapper.aio import Connection


async def main():
    db = await Connection(host='127.0.0.1', user='root', db='example', autocreate=True, read_commited=True)

    await db.book.add_column('id', 'int', primary=True, auto_increment=True, exist_ok=True)
    await db.book.add_column('name', 'text', exist_ok=True)
    await db.book.add_column('value', 'int', exist_ok=True)

    await db.book.insert({'name': 'ubuntu', 'value': 16})
    await db.commit()
    d = await db.book.find_one(1)
    print(d)

    await db.book.update(1, {'value': 18})
    await db.commit()

    for d in await db.book.find({'name': 'ubuntu'}):
        print(d)
    
    await db.book.delete({'value': 18})
    await db.commit()

    print(await db.book.count())

    async for name in db:
        print('table', name)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
