import pytest
from asyncpg.connection import Connection
from migration_driver import AsyncpgMigrationDriver


@pytest.fixture()
def driver(conn: Connection) -> AsyncpgMigrationDriver:
    return AsyncpgMigrationDriver(conn)


@pytest.mark.asyncio
async def test_saving_migration(driver: AsyncpgMigrationDriver, conn: Connection):
    await driver.save_migration("001")
    migrations = await conn.fetch(f'select * from "{AsyncpgMigrationDriver.table_name}"')
    assert len(migrations) == 1
    assert migrations[0].get('id') == '001'


@pytest.mark.asyncio
async def test_saving_migration_many(driver: AsyncpgMigrationDriver, conn: Connection):
    migration_ids = ['1', '2', '3', '4']
    for m in migration_ids:
        await driver.save_migration(m)
    migrations = await conn.fetch(f'select * from "{AsyncpgMigrationDriver.table_name}" order by "order"')
    assert len(migrations) == len(migration_ids)
    for i, (mid, m) in enumerate(zip(migration_ids, migrations)):
        assert mid == m.get('id')
        assert i + 1 == m.get('order')


@pytest.mark.asyncio
async def test_get_last_migration_should_return_last_migration_if_exists(driver: AsyncpgMigrationDriver):
    migration_ids = ['1', '2', '3', '4']
    for m in migration_ids:
        await driver.save_migration(m)

    assert await driver.get_last_migration() == '4'


@pytest.mark.asyncio
async def test_get_last_migration_should_return_none_if_does_not_exist(driver: AsyncpgMigrationDriver):
    assert await driver.get_last_migration() is None


@pytest.mark.asyncio
async def test_get_last_migration_should_return_none_if_does_not_exist_2(
        driver: AsyncpgMigrationDriver, conn: Connection
):
    await driver.save_migration('1')
    await conn.execute(f'delete from "{AsyncpgMigrationDriver.table_name}"')
    assert await driver.get_last_migration() is None


@pytest.mark.asyncio
async def test_get_connection_should_return_connection_object(driver: AsyncpgMigrationDriver):
    assert await driver.get_connection() is not None
