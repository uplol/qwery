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


class ComplexPostgresModel(Model):
    class Meta:
        table_name = "complex"

    a: JSONB[Optional[EmbeddedData]]


async def test_postgres_conn(conn):
    rows = await conn.fetch("SELECT 1")
    assert len(rows) == 1
    assert rows[0][0] == 1


async def test_postgres_insert(conn):
    fn = Query(SimplePostgresModel).insert().execute()
    await fn(conn, a=1, b="test", c=True)
