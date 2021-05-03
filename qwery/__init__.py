import dataclasses
import json
import string
import typing
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from pydantic import BaseModel, create_model
from pydantic.fields import ModelField


class ModelNotFound(Exception):
    pass


class ModelValidationError(Exception):
    pass


class ModelMeta:
    table_name: str


class Model(BaseModel):
    if TYPE_CHECKING:
        Meta: ModelMeta


JSONContainerType = TypeVar("JSONContainerType")


class JSONB(Generic[JSONContainerType]):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v, field: ModelField):
        if isinstance(v, (str, bytes)):
            v = json.loads(v)
        assert field.sub_fields is not None
        return field.sub_fields[0].validate(v, {}, loc=field.name)[0]


@dataclass
class Arg:
    type_: Any
    splat: Optional[Callable[[Any], List[Any]]]


class Method:
    def __init__(self, query):
        self._query = query
        argument_model = self._query.build_argument_model()
        self._argument_model = argument_model

    def _process_arguments(self, *, arguments: Dict[str, Any]) -> List[Any]:
        inst = self._argument_model(**arguments)
        result = []
        for key, field_obj in inst.__fields__.items():
            value = getattr(inst, key)
            if "jsonb" in field_obj.field_info.extra:
                if isinstance(value, dict):
                    value = json.dumps(value)
                else:
                    value = value.json()
            result.append(value)
        return result

    @property
    def sql(self):
        return self._query.sql


class ExecuteMethod(Method):
    async def __call__(self, conn, **kwargs):
        args = self._process_arguments(arguments=kwargs)
        await conn.execute(self.sql, *args)


class PrepareMethod(Method):
    async def __call__(self, conn, **kwargs):
        args = self._process_arguments(arguments=kwargs)
        return await conn.prepare(self.sql, *args)


class FetchOneMethod(Method):
    async def __call__(self, conn, **kwargs):
        args = self._process_arguments(arguments=kwargs)
        result = await conn.fetchrow(self.sql, *args)
        if not result:
            raise ModelNotFound
        return self._query.model(**dict(result))


class FetchAllMethod(Method):
    async def __call__(self, conn, **kwargs):
        return [self._query.model(**dict(i)) async for i in self.rows(conn, **kwargs)]

    async def tuples(self, conn, **kwargs):
        return [tuple(i) async for i in self.rows(conn, **kwargs)]

    async def rows(self, conn, **kwargs):
        args = self._process_arguments(arguments=kwargs)
        for i in await conn.fetch(self.sql, *args):
            yield i

    async def cursor(self, conn, **kwargs):
        args = self._process_arguments(arguments=kwargs)
        async for i in conn.cursor(self.sql, *args):
            yield self._query.model(**dict(i))


QueryBuilderT = TypeVar("QueryBuilderT", bound="QueryBuilder")


SUPPORTED_ARGUMENT_TYPE_HINTS = {
    "int": int,
    "str": str,
}


@dataclass(frozen=True)
class QueryBuilder:
    @dataclass(frozen=True)
    class QueryArgument:
        # The argument name which will be used to fetch an argument value from the
        #  called kwargs.
        name: str

        # The underlying type of this argument. This is passed to pydantic for
        #  validating argument input.
        type: object

    # The model this query is for
    model: Any
    sql: str
    args: List[QueryArgument] = field(default_factory=list)

    # Whether this query as it stands will return any data (either SELECT, or
    #  we have some RETURNING clause)
    returns_data: bool = False

    def _with_sql(self: QueryBuilderT, contents) -> QueryBuilderT:
        if self.sql:
            contents = " " + contents
        return dataclasses.replace(self, sql=self.sql + contents)

    def _with_arg(
        self: QueryBuilderT, name: str, type_of: object = Any
    ) -> Tuple[str, QueryBuilderT]:
        for index, arg in enumerate(self.args):
            if arg.name == name:
                return f"${index + 1}", self

        if typing.get_origin(type_of) == JSONB:
            pass

        return (
            f"${len(self.args) + 1}",
            dataclasses.replace(
                self,
                args=self.args + [QueryBuilder.QueryArgument(name=name, type=type_of)],
            ),
        )

    def _with_returns_data(self: QueryBuilderT) -> QueryBuilderT:
        return dataclasses.replace(self, returns_data=True)

    def _parse_arguments(
        self: QueryBuilderT, contents: str
    ) -> Tuple[str, QueryBuilderT]:
        format_parts = list(string.Formatter().parse(contents))

        # No actual format parts where in the string, so we don't need to do anything
        if len(format_parts) == 1 and not format_parts[0][1]:
            return format_parts[0][0], self

        output_contents = ""
        for (unrelated_text, field_contents, typehint, y) in format_parts:
            output_contents += unrelated_text
            if not field_contents:
                continue

            type = Any

            if field_contents.startswith("."):
                assert (
                    not typehint
                ), "model field reference arg cannot also have a type hint"
                field_contents = field_contents[1:]
                type = typing.get_type_hints(self.model)[field_contents]

            if typehint:
                type = SUPPORTED_ARGUMENT_TYPE_HINTS[typehint.strip()]

            arg_ref, self = self._with_arg(field_contents, type_of=type)
            output_contents += arg_ref

        return output_contents, self

    def offset(self: QueryBuilderT, amount: Union[int, str]) -> QueryBuilderT:
        if isinstance(amount, str):
            amount, self = self._parse_arguments(amount)

        return self._with_sql(f"OFFSET {amount}")

    def limit(self: QueryBuilderT, amount: Union[int, str]) -> QueryBuilderT:
        if isinstance(amount, str):
            amount, self = self._parse_arguments(amount)

        return self._with_sql(f"LIMIT {amount}")

    def raw(self: QueryBuilderT, sql: str) -> QueryBuilderT:
        sql, self = self._parse_arguments(sql)
        return self._with_sql(sql)

    def fetch_one(self) -> FetchOneMethod:
        if self.returns_data is False:
            raise ValueError(
                "cannot call fetch_one on a query that does not return data"
            )
        return FetchOneMethod(self)

    def fetch_all(self) -> FetchAllMethod:
        if self.returns_data is False:
            raise ValueError(
                "cannot call fetch_one on a query that does not return data"
            )
        return FetchAllMethod(self)

    def prepare(self) -> PrepareMethod:
        return PrepareMethod(self)

    def execute(self) -> ExecuteMethod:
        return ExecuteMethod(self)

    def build_argument_model(self):
        model = create_model(
            f"{self.model.__name__}QueryArgs",
            **{arg.name: (arg.type, ...) for arg in self.args},
        )

        for arg in self.args:
            if typing.get_origin(arg.type) == JSONB:
                model.__fields__[arg.name].field_info.extra["jsonb"] = True
        return model


class Query:
    def __init__(self, model):
        self.model = model

    def select(self, *, selection=None, alias=None) -> "SelectQueryBuilder":
        field_prefix = alias + "." if alias else ""

        if not selection:
            selection = ", ".join(
                [
                    field_prefix + field_name
                    for field_name in self.model.__fields__.keys()
                ]
            )

        table_name = self.model.Meta.table_name

        if alias is not None:
            table_name = f"{table_name} {alias}"

        return SelectQueryBuilder(
            model=self.model,
            sql=f"SELECT {selection} FROM {table_name}",
            returns_data=True,
        )

    def delete(self) -> "DeleteQueryBuilder":
        return DeleteQueryBuilder(
            model=self.model, sql=f"DELETE FROM {self.model.Meta.table_name}"
        )

    def update(self, *args, **kwargs) -> "UpdateQueryBuilder":
        builder = UpdateQueryBuilder(model=self.model, sql="")

        set_statements = []
        for arg in args:
            arg_ref, builder = builder._with_arg(
                arg, typing.get_type_hints(self.model)[arg]
            )
            set_statements.append(f"{arg} = {arg_ref}")

        for key, value in kwargs.items():
            value, builder = builder._parse_arguments(value)
            set_statements.append(f"{key} = {value}")

        return builder._with_sql(
            f"UPDATE {self.model.Meta.table_name} SET {', '.join(set_statements)}"
        )

    def insert(
        self, *, exclude: Optional[Set[str]] = None, **kwargs
    ) -> "InsertQueryBuilder":
        """
        Constructs an insert query out of all fields in the model. Any fields in
        `excluded` will be skipped. Any kwargs passed will be treated as additional
        field setters with expressions.
        """
        builder = InsertQueryBuilder(model=self.model, sql="")

        args = {}
        for field_name in self.model.__fields__.keys():
            if exclude and field_name in exclude:
                continue

            if field_name in kwargs:
                continue

            arg_ref, builder = builder._with_arg(
                field_name, typing.get_type_hints(self.model)[field_name]
            )
            args[field_name] = arg_ref

        for field_name, field_value in kwargs.items():
            field_value, builder = builder._parse_arguments(field_value)
            args[field_name] = field_value

        fields = ", ".join(args.keys())
        values = ", ".join(args.values())
        return builder._with_sql(
            f"INSERT INTO {self.model.Meta.table_name} ({fields}) VALUES ({values})"
        )


def _returning_mixin_fn(self: QueryBuilderT) -> QueryBuilderT:
    return self._with_sql("RETURNING *")._with_returns_data()


def _where_mixin_fn(self: QueryBuilderT, query: str) -> QueryBuilderT:
    query, self = self._parse_arguments(query)
    return self._with_sql(f"WHERE {query}")


class SelectQueryBuilder(QueryBuilder):
    where = _where_mixin_fn

    def group_by(self: QueryBuilderT, target) -> QueryBuilderT:
        target, self = self._parse_arguments(target)
        return self._with_sql(f"GROUP BY {target}")

    def order_by(self: QueryBuilderT, target, *, direction="ASC") -> QueryBuilderT:
        target, self = self._parse_arguments(target)
        direction, self = self._parse_arguments(direction)
        return self._with_sql(f"ORDER BY {target} {direction}")

    def join(
        self: QueryBuilderT,
        target_model: Union[Model, str],
        on: str,
        *,
        alias: str = None,
        direction: str = None,
    ) -> QueryBuilderT:
        if not isinstance(target_model, str):
            target_model = target_model.Meta.table_name

        if alias is not None:
            target_model += f" {alias}"

        on, self = self._parse_arguments(on)

        direction = direction or ""
        if direction:
            direction = direction + " "

        return self._with_sql(f"{direction}JOIN {target_model} ON {on}")


class DeleteQueryBuilder(QueryBuilder):
    where = _where_mixin_fn
    returning = _returning_mixin_fn


class UpdateQueryBuilder(QueryBuilder):
    where = _where_mixin_fn
    returning = _returning_mixin_fn


class InsertQueryBuilder(QueryBuilder):
    returning = _returning_mixin_fn

    def on_conflict(
        self: QueryBuilderT, conflict: str, *, action: str = "DO NOTHING"
    ) -> QueryBuilderT:
        action, self = self._parse_arguments(action)
        return self._with_sql(f"ON CONFLICT ({conflict}) {action}")
