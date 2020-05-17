import pytest
from pydantic import ValidationError
from typing import Optional
from qwery import Model, Query


class ExampleModel(Model):
    class Meta:
        table_name = "test"

    a: int
    b: Optional[str]
    c: bool


def test_compile_select_query():
    test_model_query = Query(ExampleModel).select().where("a = {.a}").fetchone()
    assert test_model_query.sql == "SELECT a, b, c FROM test WHERE a = $1 LIMIT 1"
    assert callable(test_model_query.build())

    test_model_query_complex = (
        Query(ExampleModel).select(raw="COUNT(*)").where("a = {.a}").fetchone()
    )
    assert (
        test_model_query_complex.sql == "SELECT COUNT(*) FROM test WHERE a = $1 LIMIT 1"
    )
    assert callable(test_model_query_complex.build())


def test_compile_insert_query():
    test_model_query = Query(ExampleModel).insert()
    assert test_model_query.sql == "INSERT INTO test (a, b, c) VALUES ($1, $2, $3)"
    assert callable(test_model_query.build())

    test_model_query_complex = Query(ExampleModel).insert().on_conflict("a").returning()
    assert (
        test_model_query_complex.sql
        == "INSERT INTO test (a, b, c) VALUES ($1, $2, $3) ON CONFLICT (a) DO NOTHING RETURNING *"
    )
    assert callable(test_model_query_complex.build())


def test_compile_delete_query():
    test_model_query = Query(ExampleModel).delete().where("a = {.a}")
    assert test_model_query.sql == "DELETE FROM test WHERE a = $1"
    assert callable(test_model_query.build())


def test_compile_update_query():
    test_model_query = Query(ExampleModel).update("b", "a", "c").where("a = {.a}")
    assert test_model_query.sql == "UPDATE test SET b = $1, a = $2, c = $3 WHERE a = $2"
    assert callable(test_model_query.build())


@pytest.mark.asyncio
async def test_validate_select_query():
    test_model_query = Query(ExampleModel).select().where("a = {.a}").fetchone()
    with pytest.raises(ValidationError):
        await test_model_query.build()(None, a="fuck")


@pytest.mark.asyncio
async def test_validate_insert_query():
    test_model_query = Query(ExampleModel).insert(ignore={"b"})
    with pytest.raises(ValidationError) as excinfo:
        await test_model_query.build()(None, a=1, c="yeet")

    assert excinfo.value.errors() == [
        {
            "loc": ("c",),
            "msg": "value could not be parsed to a boolean",
            "type": "type_error.bool",
        }
    ]

    test_model_query = Query(ExampleModel).insert(body=True)
    with pytest.raises(ValidationError) as excinfo:
        await test_model_query.build()(None, a=1, c=1)

    assert excinfo.value.errors() == [
        {
            "loc": ("example_model",),
            "msg": "field required",
            "type": "value_error.missing",
        }
    ]
