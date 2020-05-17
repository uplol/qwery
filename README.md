# qwery

qwery is a small and lightweight query builder based on asyncpg and pydantic.

## why a query builder

In my opinion query builders strike a great balance between the flexibility of raw SQL, the structure and safety of pre-crafted queries, and the comfortable data layer of an ORM.

These benefits come with some downsides:

- You lose some flexibility when crafting queries, especially when dealing with things like partial updates.
- While the query builder interface does provide _some_ typing, its dynamic nature means it can never match the safety of pre-crafted queries with hand-written or generated types.
- Complex queries returning non-standard data become unruly fast.

## model, queries, helper pattern

qwery works best with a model + queries + helper pattern, namely:

- Models describe only data and how it is stored
- Queries describe how models interact with the database
- Helpers describe and implement the interaction _between_ models and the application (creation, fetching, etc)

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
    create = Query(MyModel).insert(body=True).build()
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
