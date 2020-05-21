import string
from asyncpg import Connection
from asyncpg.prepared_stmt import PreparedStatement
from dataclasses import dataclass
from typing import (
    TypeVar,
    Type,
    Callable,
    Iterable,
    Any,
    Generic,
    Dict,
    Awaitable,
    List,
    Optional,
    Tuple,
    Set,
)
from pydantic import BaseModel, ValidationError, create_model
from pydantic.fields import ModelField


class ModelNotFound(Exception):
    pass


class ModelValidationError(Exception):
    pass


class Model(BaseModel):
    pass


QueryT = TypeVar("QueryT")
T = TypeVar("T", bound=Model)


def _get_field_type(field: ModelField):
    type_ = field.outer_type_
    if not field.required:
        type_ = Optional[type_]
    return type_


@dataclass
class Arg:
    type_: Any
    splat: Optional[Callable[[Any], List[Any]]]


class BaseSubQuery(Generic[T]):
    model: Type[T]
    sql: str
    args: Dict[str, Arg]
    idx: int

    def __init__(self, model: Type[T], sql: str, args: Dict[str, Arg], idx: int):
        self.model = model
        self.sql = sql
        self.args = args
        self.idx = idx

    def generate_sql_args(self, kwargs: Dict[str, Any]) -> List[Any]:
        args = []
        for k, v in self.args.items():
            if v.splat:
                args.extend(v.splat(kwargs[k]))
            else:
                args.append(kwargs[k])
        return args


class PreperQuery(Generic[T], BaseSubQuery[T]):
    def build(self) -> Callable[..., Awaitable[PreparedStatement]]:
        async def f(conn: Connection) -> PreparedStatement:
            return await conn.prepare(self.sql)

        f.__name__ = "prepare_query"
        return f


class ExecuteQuery(Generic[T], BaseSubQuery[T]):
    def build(self) -> Callable[..., Awaitable[T]]:
        value: Any = {k: (v.type_, ...) for k, v in self.args.items()}
        ArgsModel = create_model("ExecuteArgsModel", **value)

        async def f(conn: Connection, **kwargs):
            parsed_kwargs = ArgsModel(**kwargs).dict()
            await conn.execute(self.sql, *self.generate_sql_args(parsed_kwargs))

        f.__name__ = "execute_query"
        return f


class FetchOneQuery(Generic[T], BaseSubQuery[T]):
    def build(self) -> Callable[..., Awaitable[T]]:
        value: Any = {k: (v.type_, ...) for k, v in self.args.items()}
        ArgsModel = create_model("FetchOneArgsModel", **value)

        async def f(conn: Connection, **kwargs) -> T:
            parsed_kwargs = ArgsModel(**kwargs).dict()
            result = await conn.fetchrow(
                self.sql, *self.generate_sql_args(parsed_kwargs)
            )
            if not result:
                raise ModelNotFound
            try:
                return self.model.parse_obj(dict(result))
            except ValidationError as e:
                raise ModelValidationError(str(e))

        f.__name__ = "execute_fetch_one_query"
        return f


class FetchAllQuery(Generic[T], BaseSubQuery[T]):
    def build(self) -> Callable[..., Awaitable[Iterable[T]]]:
        value: Any = {k: (v.type_, ...) for k, v in self.args.items()}
        ArgsModel = create_model("FetchAllArgsModel", **value)

        async def f(conn: Connection, **kwargs) -> Iterable[T]:
            parsed_kwargs = ArgsModel(**kwargs).dict()
            try:
                return [
                    self.model.parse_obj(dict(i))
                    for i in await conn.fetch(
                        self.sql, *self.generate_sql_args(parsed_kwargs)
                    )
                ]
            except ValidationError as e:
                raise ModelValidationError(str(e))

        f.__name__ = "execute_fetch_one_query"
        return f


class FetchValQuery(Generic[T], BaseSubQuery[T]):
    def build(self) -> Callable[..., Awaitable[Any]]:
        value: Any = {k: (v.type_, ...) for k, v in self.args.items()}
        ArgsModel = create_model("FetchValArgsModel", **value)

        async def f(conn: Connection, **kwargs) -> Any:
            parsed_kwargs = ArgsModel(**kwargs).dict()
            return await conn.fetchval(self.sql, *self.generate_sql_args(parsed_kwargs))

        f.__name__ = "execute_fetch_val_query"
        return f


class TuplesQuery(Generic[T], BaseSubQuery[T]):
    def build(self) -> Callable[..., Awaitable[Iterable[T]]]:
        value: Any = {k: (v.type_, ...) for k, v in self.args.items()}
        ArgsModel = create_model("FetchTuplesArgsModel", **value)

        async def f(conn: Connection, **kwargs) -> Iterable[T]:
            parsed_kwargs = ArgsModel(**kwargs).dict()
            return await conn.fetch(self.sql, *self.generate_sql_args(parsed_kwargs))

        f.__name__ = "execute_tuples_query"
        return f


def _where(
    query: BaseSubQuery[T], raw_where_query: str
) -> Tuple[Type[T], str, Dict[str, Arg], int]:
    sql = query.sql.strip() + " WHERE "

    idx = query.idx
    args = query.args.copy()
    for i, (text, field, _, _) in enumerate(string.Formatter().parse(raw_where_query)):
        sql += text

        if field:
            if field.startswith("."):
                # This field is typed as a reference to the root model
                field = field[1:]
                arg = Arg(_get_field_type(query.model.__fields__[field]), None)
            else:
                arg = Arg(Any, None)

            if field in args:
                index = list(args.keys()).index(field)
                # TODO: tyep check?
                sql += f"${index + 1}"
                continue

            args[field] = arg
            idx += 1
            sql += f"${idx}"

    return (query.model, sql, args, idx)


class SelectQuery(Generic[T], BaseSubQuery[T]):
    def where(self, raw_where_query) -> "SelectQuery[T]":
        return SelectQuery[T](*_where(self, raw_where_query))

    def fetchone(self) -> FetchOneQuery[T]:
        return FetchOneQuery(
            self.model, self.sql.strip() + " LIMIT 1", self.args.copy(), self.idx
        )

    def fetchval(self) -> FetchValQuery[T]:
        return FetchValQuery(self.model, self.sql, self.args.copy(), self.idx)

    def fetchall(self) -> FetchAllQuery[T]:
        return FetchAllQuery(self.model, self.sql, self.args.copy(), self.idx)

    def tuples(self) -> TuplesQuery[T]:
        return TuplesQuery(self.model, self.sql, self.args.copy(), self.idx)

    def group_by(self, by) -> "SelectQuery[T]":
        return SelectQuery[T](
            self.model, self.sql.strip() + f" GROUP BY {by}", self.args.copy(), self.idx
        )

    def order_by(self, by, direction="ASC") -> "SelectQuery[T]":
        return SelectQuery[T](
            self.model,
            self.sql.strip() + f" ORDER BY {by} {direction}",
            self.args.copy(),
            self.idx,
        )

    def preper(self) -> PreperQuery[T]:
        return PreperQuery(self.model, self.sql, self.args.copy(), self.idx)

    def join(self, other_model, on) -> "SelectQuery[T]":
        sql = (
            self.sql.strip()
            + f" JOIN {other_model.Meta.table_name} {other_model.Meta.table_name} ON {on}"
        )
        return SelectQuery[T](self.model, sql, self.args.copy(), self.idx)


class DeleteQuery(Generic[T], BaseSubQuery[T]):
    def where(self, raw_where_query: str) -> "DeleteQuery[T]":
        return DeleteQuery(*_where(self, raw_where_query))

    def build(self) -> Callable[..., Awaitable[T]]:
        return ExecuteQuery[T](self.model, self.sql, self.args, self.idx).build()


class UpdateQuery(Generic[T], BaseSubQuery[T]):
    def where(self, raw_where_query: str) -> "UpdateQuery[T]":
        return UpdateQuery(*_where(self, raw_where_query))

    def returning(self) -> "UpdateQuery[T]":
        return UpdateQuery[T](
            self.model, self.sql.strip() + " RETURNING *", self.args.copy(), self.idx
        )

    def fetchone(self) -> FetchOneQuery[T]:
        return FetchOneQuery(self.model, self.sql, self.args.copy(), self.idx)

    def fetchall(self) -> FetchAllQuery[T]:
        return FetchAllQuery(self.model, self.sql, self.args.copy(), self.idx)

    def build(self) -> Callable[..., Awaitable[T]]:
        return ExecuteQuery[T](self.model, self.sql, self.args, self.idx).build()


class InsertQuery(Generic[T], BaseSubQuery[T]):
    def on_conflict(self, col, action="DO NOTHING") -> "InsertQuery[T]":
        return InsertQuery[T](
            self.model,
            self.sql.strip() + f" ON CONFLICT ({col}) {action}",
            self.args,
            self.idx,
        )

    def fetchone(self) -> FetchOneQuery[T]:
        return FetchOneQuery(self.model, self.sql, self.args.copy(), self.idx)

    def fetchall(self) -> FetchAllQuery[T]:
        return FetchAllQuery(self.model, self.sql, self.args.copy(), self.idx)

    def returning(self) -> "InsertQuery[T]":
        return InsertQuery[T](
            self.model, self.sql.strip() + " RETURNING *", self.args.copy(), self.idx
        )

    def build(self) -> Callable[..., Awaitable[T]]:
        return ExecuteQuery[T](self.model, self.sql, self.args, self.idx).build()


class Query(Generic[T]):
    def __init__(self, model: Type[T]):
        self.model = model

    @property
    def _table_name(self):
        return self.model.Meta.table_name

    def delete(self) -> DeleteQuery[T]:
        return DeleteQuery[T](self.model, f"DELETE FROM {self._table_name}", {}, 0)

    def update(self, *fields, **kwargs) -> UpdateQuery[T]:
        args = {}
        updates = []
        idx = 0

        for field in fields:
            args[field] = Arg(_get_field_type(self.model.__fields__[field]), None)
            idx += 1
            updates.append(f"{field} = ${idx}")

        for k, v in kwargs.items():
            updates.append(f"{k} = {v}")

        return UpdateQuery[T](
            self.model, f"UPDATE {self._table_name} SET {', '.join(updates)}", args, idx
        )

    def insert(
        self, body: bool = False, ignore: Optional[Set[str]] = None
    ) -> ExecuteQuery[T]:
        if body:
            assert ignore is None, "cannot use ignore with body = True"

            def f(inst: Dict[str, Any]):
                return inst.values()

            camel = ""
            for char in self.model.__name__:
                if camel and char.isupper():
                    camel += "_" + char.lower()
                else:
                    camel += char.lower()

            f.__name__ = f"{camel}_splat"
            fields = list(self.model.__fields__.keys())
            args = {camel: Arg(self.model, f)}
        else:
            fields = [
                k for k in self.model.__fields__.keys() if not ignore or k not in ignore
            ]

            args = {}
            for field in fields:
                args[field] = Arg(_get_field_type(self.model.__fields__[field]), None)

        values = ", ".join(f"${idx + 1}" for idx in range(len(fields)))
        sql = f"INSERT INTO {self._table_name} ({', '.join(fields)}) VALUES ({values})"
        return InsertQuery[T](self.model, sql, args, len(fields))

    def select(self, raw=None, alias=None) -> SelectQuery[T]:
        fields = raw or ", ".join(
            [alias + "." + i if alias else i for i in self.model.__fields__.keys()]
        )
        sql = f"SELECT {fields} FROM {self._table_name}{' ' + (alias or '')}"
        return SelectQuery[T](self.model, sql, {}, 0)
