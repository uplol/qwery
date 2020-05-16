# qwery

qwery is a small and lightweight query builder based on asyncpg and pydantic.

## example

```py
from pydantic import BaseModel
from qwery import Query


class MyModel(BaseModel):
    class Meta:
        table_name = "my_table"

    id: int
    name: str
    desc: Optional[str]
    active: bool


class MyModelQueries:
    create = Query(MyModel).insert().build()
    delete_by_id = Query(MyModel).delete().where("id = {.id}").build()
    get_by_id = Query(MyModel).select().where("id = {.id}").fetchone().build()
    get_all = Query(MyModel).select().fetchall().build()


async with pool.acquire() as conn:
    model = MyModel(id=1, name="test", desc=None, active=True)
    await MyModelQueries.create(conn, model=model)

    model = await MyModelQueries.get_by_id(conn, id=1)
    models = await MyModelQueries.get_all(conn)
    assert models == [model]

    await MyModelQueries.delete(conn, id=1)
```
