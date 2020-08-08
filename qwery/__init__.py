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
from pydantic import BaseModel, create_model
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


class Method:
    def __init__(self, query):
        self._query = query

    @property
    def sql(self):
        return self._query.sql


class ExecuteMethod(Method):
    async def __call__(self, conn, **kwargs):
        sql, args = self._query.build(**kwargs)
        await conn.execute(sql, *args)


class PrepareMethod(Method):
    async def __call__(self, conn, **kwargs):
        sql, args = self._query.build(**kwargs)
        return await conn.prepare(sql)


class FetchOneMethod(Method):
    async def __call__(self, conn, **kwargs):
        sql, args = self._query.build(**kwargs)
        result = await conn.fetchrow(sql, *args)
        if not result:
            raise ModelNotFound
        return self._query.model.construct(**dict(result))


class FetchAllMethod(Method):
    async def __call__(self, conn, **kwargs):
        sql, args = self._query.build(**kwargs)
        return [
            self._query.model.construct(**dict(i)) for i in await conn.fetch(sql, *args)
        ]

    async def tuples(self, conn, **kwargs):
        sql, args = self._query.build(**kwargs)
        return [tuple(i) for i in await conn.fetch(sql, *args)]


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

        self._args_model = None

    @property
    def args_model(self):
        if not self._args_model:
            value: Any = {k: (v.type_, ...) for k, v in self.args.items()}
            self._args_model = create_model("ArgsModel", **value)
        return self._args_model

    def execute(self) -> ExecuteMethod:
        return ExecuteMethod(self)

    def prepare(self) -> PrepareMethod:
        return PrepareMethod(self)

    def fetch_one(self) -> FetchOneMethod:
        return FetchOneMethod(self)

    def fetch_all(self) -> FetchAllMethod:
        return FetchAllMethod(self)

    def limit(self, amount):
        return self.__class__(
            self.model, self.sql + f" LIMIT {amount}", self.args.copy(), self.idx
        )

    def returning(self):
        return self.__class__(
            self.model, self.sql.strip() + " RETURNING *", self.args.copy(), self.idx
        )

    def build(self, **kwargs):
        parsed_kwargs = self.args_model(**kwargs).dict()
        return self.sql, self.generate_sql_args(parsed_kwargs)

    def generate_sql_args(self, kwargs: Dict[str, Any]) -> List[Any]:
        args = []
        for k, v in self.args.items():
            if v.splat:
                args.extend(v.splat(kwargs[k]))
            else:
                args.append(kwargs[k])
        return args


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

    def raw(self, raw) -> "SelectQuery[T]":
        return SelectQuery[T](
            self.model, self.sql.strip() + " " + raw, self.args.copy(), self.idx
        )

    def join(self, other_model, on, alias=None, direction=None) -> "SelectQuery[T]":
        sql = (
            self.sql.strip()
            + f" {direction or ''} JOIN {other_model.Meta.table_name} {alias or other_model.Meta.table_name} ON {on}"
        )
        return SelectQuery[T](self.model, sql, self.args.copy(), self.idx)


class DeleteQuery(Generic[T], BaseSubQuery[T]):
    def where(self, raw_where_query: str) -> "DeleteQuery[T]":
        return DeleteQuery(*_where(self, raw_where_query))


class UpdateQuery(Generic[T], BaseSubQuery[T]):
    def where(self, raw_where_query: str) -> "UpdateQuery[T]":
        return UpdateQuery(*_where(self, raw_where_query))

    def returning(self) -> "UpdateQuery[T]":
        return UpdateQuery[T](
            self.model, self.sql.strip() + " RETURNING *", self.args.copy(), self.idx
        )


class InsertQuery(Generic[T], BaseSubQuery[T]):
    def on_conflict(self, col, action="DO NOTHING") -> "InsertQuery[T]":
        return InsertQuery[T](
            self.model,
            self.sql.strip() + f" ON CONFLICT ({col}) {action}",
            self.args,
            self.idx,
        )

    def returning(self) -> "InsertQuery[T]":
        return InsertQuery[T](
            self.model, self.sql.strip() + " RETURNING *", self.args.copy(), self.idx
        )


class DynamicUpdateQuery(Generic[T], BaseSubQuery[T]):
    def where(self, raw_where_query: str) -> "DynamicUpdateQuery[T]":
        return DynamicUpdateQuery(*_where(self, raw_where_query))

    def build(self, **kwargs):
        parsed_kwargs = self.args_model(
            **{k: kwargs.pop(k) for k in self.args_model.__fields__}
        ).dict()
        parts = [f"{k} = ${self.idx + i + 1}" for i, k in enumerate(kwargs.keys())]
        sql = self.sql.format(dynamic=", ".join(parts))
        args = self.generate_sql_args(parsed_kwargs) + list(kwargs.values())
        return sql, args


class Query(Generic[T]):
    def __init__(self, model: Type[T]):
        self.model = model

    @property
    def _table_name(self):
        return self.model.Meta.table_name

    def delete(self) -> DeleteQuery[T]:
        return DeleteQuery[T](self.model, f"DELETE FROM {self._table_name}", {}, 0)

    def dynamic_update(self) -> DynamicUpdateQuery[T]:
        return DynamicUpdateQuery[T](
            self.model, f"UPDATE {self._table_name} SET {{dynamic}} ", {}, 0
        )

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
    ) -> InsertQuery[T]:
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
