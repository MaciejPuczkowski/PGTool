from typing import Any, Optional

import asyncpg.connection
import pytest
import pytest_asyncio
from migrator import Migrator, Migration, AsyncpgMigrator
from migration_driver import MigrationDriver
from exceptions import MigrationIncorrectHead


class TConnection:
    pass


class TMigrationDriver(MigrationDriver):
    def __init__(self):
        self._migrations = []

    async def get_last_migration(self) -> Optional[str]:
        if self._migrations:
            return self._migrations[-1]
        return None

    async def get_connection(self) -> TConnection:
        pass

    async def save_migration(self, id: str):
        self._migrations.append(id)


def migrator_fake():
    return Migrator(TMigrationDriver())


def asyncpg_migrator(conn: asyncpg.connection.Connection):
    return AsyncpgMigrator(conn)


def pytest_generate_tests(metafunc):
    if "migrator" in metafunc.fixturenames:
        metafunc.parametrize("migrator", ["fake", "asyncpg"], indirect=True)


@pytest_asyncio.fixture()
def migrator(request, conn: asyncpg.connection.Connection):
    return {
        'fake': migrator_fake(),
        'asyncpg': asyncpg_migrator(conn)
    }.get(request.param)

@pytest.mark.asyncio
async def test_migrator_migrate_to_head_with_one_migration(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration.sql'))
    migrated = [migration async for migration in migrator.migrate()]
    last_migrated = await migrator.get_last_applied_migration()
    assert last_migrated.id == '001_first_migration.sql'
    assert migrated == ['001_first_migration.sql']


@pytest.mark.asyncio
async def test_migrator_migrate_to_head_with_no_migrations(migrator: Migrator):
    with pytest.raises(MigrationIncorrectHead):
        [migration async for migration in migrator.migrate()]


@pytest.mark.asyncio
async def test_migrator_migrate_to_not_existing_migrations_with_zero_migrations(migrator: Migrator):
    with pytest.raises(MigrationIncorrectHead):
        [migration async for migration in migrator.migrate(head='001')]


@pytest.mark.asyncio
async def test_migrator_migrate_to_not_existing_migration(migrator: Migrator):
    with pytest.raises(MigrationIncorrectHead):
        migrator.use_migration(Migration(id='001_first_migration'))
        [migration async for migration in migrator.migrate(head='002')]


@pytest.mark.asyncio
async def test_migrator_to_target_linear(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['003']))
    migrated = [migration async for migration in migrator.migrate(head='004')]
    assert migrated == ['001_first_migration', '002', '003', '004']


@pytest.mark.asyncio
async def test_migrator_to_target_linear_cannot_have_repetitions(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['003', '002']))
    migrated = [migration async for migration in migrator.migrate(head='004')]
    assert migrated == ['001_first_migration', '002', '003', '004']


@pytest.mark.asyncio
async def test_migrator_to_head_linear(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['003']))
    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['001_first_migration', '002', '003', '004']


@pytest.mark.asyncio
async def test_migrator_to_head_with_depends_on_head(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'), depends_on_head=True)
    migrator.use_migration(Migration(id='002'), depends_on_head=True)
    migrator.use_migration(Migration(id='003'), depends_on_head=True)
    migrator.use_migration(Migration(id='004'), depends_on_head=True)
    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['001_first_migration', '002', '003', '004']


@pytest.mark.asyncio
async def test_migrator_to_head_non_linear(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['003', '002']))
    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['001_first_migration', '002', '003', '004']


@pytest.mark.asyncio
async def test_migrator_to_head_non_linear_unatacched(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['001_first_migration']))
    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['001_first_migration', '004']


@pytest.mark.asyncio
async def test_migrator_recurring_migration(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['003', '002']))
    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['001_first_migration', '002', '003', '004']

    migrator.use_migration(Migration(id='005', dependencies=['004', '002']))
    migrator.use_migration(Migration(id='006', dependencies=['005', '003']))

    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['005', '006']


@pytest.mark.asyncio
async def test_migrator_check_the_migration_chain(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['003', '002']))
    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['001_first_migration', '002', '003', '004']

    migrator.use_migration(Migration(id='005', dependencies=['004', '002']))
    migrator.use_migration(Migration(id='006', dependencies=['005', '003']))

    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['005', '006']


@pytest.mark.asyncio
async def test_migrator_check_the_migration_chain(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['003', '002']))
    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['001_first_migration', '002', '003', '004']

    migrator.use_migration(Migration(id='005', dependencies=['004', '002']))
    migrator.use_migration(Migration(id='006', dependencies=['005', '003']))

    assert migrator.get_migration_ids_sorted() == ['001_first_migration', '002', '003', '004', '005', '006']


@pytest.mark.asyncio
async def test_migrator_check_what_is_to_apply(migrator: Migrator):
    migrator.use_migration(Migration(id='001_first_migration'))
    migrator.use_migration(Migration(id='002', dependencies=['001_first_migration']))
    migrator.use_migration(Migration(id='003', dependencies=['002']))
    migrator.use_migration(Migration(id='004', dependencies=['003', '002']))
    migrated = [migration async for migration in migrator.migrate()]
    assert migrated == ['001_first_migration', '002', '003', '004']

    migrator.use_migration(Migration(id='005', dependencies=['004', '002']))
    migrator.use_migration(Migration(id='006', dependencies=['005', '003']))

    assert await migrator.get_next_migration_ids() == ['005', '006']
