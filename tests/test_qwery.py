from typing import Any, Optional

import pytest
from qwery import Model, Query, QueryBuilder


class ExampleModel(Model):
    class Meta:
        table_name = "test"

    a: int
    b: Optional[str]
    c: bool


@pytest.fixture(scope="session")
def builder() -> Query:
    return Query(ExampleModel)


@pytest.fixture(scope="session")
def select_builder(builder) -> QueryBuilder:
    return builder.select()


def test_query_builder_offset(select_builder):
    assert select_builder.offset(1).sql == "SELECT a, b, c FROM test OFFSET 1"
    assert (
        select_builder.offset("some_expression()").sql
        == "SELECT a, b, c FROM test OFFSET some_expression()"
    )
    assert (
        select_builder.offset("some_expression({a}, {b}, {c})").sql
        == "SELECT a, b, c FROM test OFFSET some_expression($1, $2, $3)"
    )

    assert (
        select_builder.offset("{offset: int}").sql
        == "SELECT a, b, c FROM test OFFSET $1"
    )
    assert select_builder.offset("{offset: int}").args == [
        QueryBuilder.QueryArgument(name="offset", type=int)
    ]


def test_select_query_builder_where(select_builder):
    assert (
        select_builder.where("a = {.a} AND y = {y}").sql
        == "SELECT a, b, c FROM test WHERE a = $1 AND y = $2"
    )
    assert select_builder.where("a = {.a} AND b = {.b} AND y = {y}").args == [
        QueryBuilder.QueryArgument(name="a", type=int),
        QueryBuilder.QueryArgument(name="b", type=Optional[str]),
        QueryBuilder.QueryArgument(name="y", type=Any),
    ]


def test_select_query_builder_group_by(select_builder):
    assert select_builder.group_by("x").sql == "SELECT a, b, c FROM test GROUP BY x"
    assert select_builder.group_by("{x}").sql == "SELECT a, b, c FROM test GROUP BY $1"
    assert select_builder.group_by("{x}").args == [
        QueryBuilder.QueryArgument(name="x", type=Any)
    ]


def test_select_query_builder_order_by(select_builder):
    assert (
        select_builder.order_by("x", direction="DESC").sql
        == "SELECT a, b, c FROM test ORDER BY x DESC"
    )
    assert (
        select_builder.order_by("{x}").sql == "SELECT a, b, c FROM test ORDER BY $1 ASC"
    )
    assert select_builder.order_by("{x}").args == [
        QueryBuilder.QueryArgument(name="x", type=Any)
    ]


def test_select_query_builder_join(select_builder):
    assert (
        select_builder.join(ExampleModel, "x = y").sql
        == "SELECT a, b, c FROM test JOIN test ON x = y"
    )
    assert (
        select_builder.join(ExampleModel, "x = {y}").sql
        == "SELECT a, b, c FROM test JOIN test ON x = $1"
    )
    assert select_builder.join(ExampleModel, "x = {y}").args == [
        QueryBuilder.QueryArgument(name="y", type=Any)
    ]


def test_delete_query_builder(builder):
    assert builder.delete().sql == "DELETE FROM test"
    assert builder.delete().where("x = {y}").sql == "DELETE FROM test WHERE x = $1"


def test_insert_query_builder(builder):
    assert builder.insert().sql == "INSERT INTO test (a, b, c) VALUES ($1, $2, $3)"
    assert builder.insert(exclude={"a", "c"}).sql == "INSERT INTO test (b) VALUES ($1)"
    assert (
        builder.insert(exclude={"a", "c"}, b="test({b})").sql
        == "INSERT INTO test (b) VALUES (test($1))"
    )


def test_update_query_builder(builder):
    assert builder.update("a", "b", "c").sql == "UPDATE test SET a = $1, b = $2, c = $3"
    assert (
        builder.update(a="{a}", b="{b}", c="my_expr()").sql
        == "UPDATE test SET a = $1, b = $2, c = my_expr()"
    )
