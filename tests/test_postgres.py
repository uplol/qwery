from typing import Optional, Set

from pydantic import BaseModel
from qwery import JSONB, Model, Query


class EmbeddedData(BaseModel):
    a: int
    b: str
    c: bool


class SimplePostgresModel(Model):
    class Meta:
        table_name = "simple"

    a: int
    b: str
    c: bool
    d: Optional[str]
    e: Set[str]


class JSONBPostgresModel(Model):
    class Meta:
        table_name = "test_jsonb"

    a: JSONB[Optional[EmbeddedData]]
    b: Optional[JSONB[EmbeddedData]]


async def test_postgres_conn(conn):
    rows = await conn.fetch("SELECT 1")
    assert len(rows) == 1
    assert rows[0][0] == 1


async def test_postgres_insert(conn):
    fn = Query(SimplePostgresModel).insert().execute()
    await fn(conn, a=1, b="test", c=True, d=None, e=["a", "b", "c"])


async def test_postgres_dynamic_update(conn):
    fn = Query(SimplePostgresModel).insert().execute()
    await fn(conn, a=13, b="test", c=True, d=None, e=[])

    fn = (
        Query(SimplePostgresModel)
        .dynamic_update()
        .where("a = {.a}")
        .returning()
        .fetch_one()
    )
    res = await fn(conn, a=13, b="test2", c=False, d="yeet gang 420", e=["a", "b", "c"])
    assert res.a == 13
    assert res.b == "test2"
    assert res.c is False
    assert res.d == "yeet gang 420"
    assert res.e == {"a", "b", "c"}


async def test_postgres_jsonb(conn):
    fn = Query(JSONBPostgresModel).insert().returning().fetch_one()
    test_obj = {"a": 1, "b": "b", "c": True}
    obj = await fn(conn, a=test_obj, b={"yes": "works"})
    assert obj.a == test_obj
    assert obj.b["yes"] == "works"

    obj2 = await fn(conn, a=test_obj, b=None)
    assert obj2.b is None

    obj3 = await fn(conn, a=JSONB.null, b=None)
    assert obj3.a is None
    assert obj3.b is None

    obj4 = await fn(conn, a=JSONB.null, b=EmbeddedData(a=1, b="2", c=False))
    assert obj4.b == {"a": 1, "b": "2", "c": False}

    fn = Query(JSONBPostgresModel).select().fetch_all()
    data = await fn(conn)
    assert isinstance(data[0].a, EmbeddedData)

    fn = Query(JSONBPostgresModel).update("a").returning().fetch_one()
    fn = Query(JSONBPostgresModel).update("a").returning().fetch_one()
    data = await fn(conn, a={"a": 2, "b": "c", "c": False})
    assert data.a.a == 2


async def test_postgres_jsonb_dynamic_update(conn):
    fn = Query(JSONBPostgresModel).dynamic_update().returning().fetch_one()
    obj = await fn(conn, b=EmbeddedData(a=2, b="3", c=True))
    assert obj.b == {"a": 2, "b": "3", "c": True}
