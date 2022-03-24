import pytest
import aiofiles
from asyncpg.connection import Connection
from migrations import FileMigration


def test_file_migration_should_have_id_based_on_path():
    migration = FileMigration(path='/')
    assert migration.path
    assert migration.id
    assert migration.path == migration.id


@pytest.mark.asyncio
async def test_file_migration_migrate_single(conn: Connection):
    path = '/tmp/gptool/001_mig.sql'
    async with aiofiles.open(path, mode='w') as f:
        await f.write("""
        create table "Test"(
            id serial constraint test_pk primary key,
            name varchar not null
        ); 
        """)
    migration = FileMigration(path=path)
    await migration.migrate(conn)
    columns = map(lambda x: (x.get('column_name'), x.get('data_type')), await conn.fetch('''
        SELECT column_name, data_type FROM information_schema.columns 
        WHERE table_name = 'Test';
    '''))
    assert set(columns) == {('id', 'integer'), ('name', 'character varying')}