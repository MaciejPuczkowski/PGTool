import shutil
import os
import pytest
import pytest_asyncio
import asyncpg
from migration_driver import AsyncpgMigrationDriver


@pytest_asyncio.fixture
async def conn() -> asyncpg.connection.Connection:
    conn: asyncpg.connection.Connection = await asyncpg.connect(
        user='pgtool', password='pgtool', database='PGToolTest', host='127.0.0.1'
    )
    yield conn
    await conn.execute(f'drop table if exists "{AsyncpgMigrationDriver.table_name}";')
    await conn.execute(f'drop table if exists "Test";')
    await conn.close()


@pytest.fixture(autouse=True)
def tmp_path():
    os.makedirs('/tmp/gptool/', exist_ok=True)
    yield
    shutil.rmtree('/tmp/gptool/', ignore_errors=True)