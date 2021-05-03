from typing import Optional

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


class JSONBPostgresModel(Model):
    class Meta:
        table_name = "test_jsonb"

    a: JSONB[Optional[EmbeddedData]]


async def test_postgres_conn(conn):
    rows = await conn.fetch("SELECT 1")
    assert len(rows) == 1
    assert rows[0][0] == 1


async def test_postgres_insert(conn):
    fn = Query(SimplePostgresModel).insert().execute()
    await fn(conn, a=1, b="test", c=True)


async def test_postgres_jsonb(conn):
    fn = Query(JSONBPostgresModel).insert().returning().fetch_one()
    test_obj = {"a": 1, "b": "b", "c": True}
    obj = await fn(conn, a=test_obj)
    assert obj.a == test_obj

    fn = Query(JSONBPostgresModel).select().fetch_all()
    data = await fn(conn)
    assert isinstance(data[0].a, EmbeddedData)

    fn = Query(JSONBPostgresModel).update("a").returning().fetch_one()
    fn = Query(JSONBPostgresModel).update("a").returning().fetch_one()
    data = await fn(conn, a={"a": 2, "b": "c", "c": False})
    assert data.a.a == 2
