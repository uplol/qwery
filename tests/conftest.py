import asyncio
import inspect
import os

import asyncpg
import pytest

DSN = os.getenv(
    "POSTGRES_DSN", "postgres://qwery_testing:qwery_testing@localhost/qwery_testing"
)
_pool = None

TESTING_TABLES = {
    "simple": """
        CREATE TABLE simple (a BIGINT, b TEXT, c BOOL);
    """,
    "test_jsonb": """
        CREATE TABLE test_jsonb (a JSONB);
    """,
}


@pytest.fixture(scope="session")
async def pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(dsn=DSN)

        async with _pool.acquire() as conn:
            for table, sql in TESTING_TABLES.items():
                await conn.execute(f"DROP TABLE IF EXISTS {table}")
                await conn.execute(sql)

    return _pool


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.get_event_loop()


@pytest.fixture(scope="session")
async def conn(pool):
    async with pool.acquire() as conn:
        yield conn


def pytest_collection_modifyitems(session, config, items):
    for item in items:
        if isinstance(item, pytest.Function) and inspect.iscoroutinefunction(
            item.function
        ):
            item.add_marker(pytest.mark.asyncio)
