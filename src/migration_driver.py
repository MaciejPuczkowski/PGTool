from abc import ABC, abstractmethod
from typing import Any, Optional
from asyncpg.connection import Connection
from asyncpg.exceptions import UndefinedTableError


class MigrationDriver(ABC):
    @abstractmethod
    async def get_last_migration(self) -> Optional[str]:
        pass

    @abstractmethod
    async def get_connection(self) -> Any:
        pass

    async def save_migration(self, id: str):
        pass


class AsyncpgMigrationDriver(MigrationDriver):
    table_name = '_PG_Migration'

    def __init__(self, conn: Connection, table_name: Optional[str] = None):
        self._table_checked = False
        self._conn: Connection = conn
        self._stmt_insert = None
        self._stmt_get_last = None
        self.table_name = self.table_name or table_name

    async def get_last_migration(self) -> Optional[str]:
        if not self._stmt_get_last:
            try:
                self._stmt_get_last = await self._conn.prepare(f'''
                    select id from "{self.table_name}" order by "order" DESC limit 1
                ''')
            except UndefinedTableError:
                return None
        rec = await self._stmt_get_last.fetchrow()
        return rec.get('id') if rec else None

    async def get_connection(self) -> Connection:
        return self._conn

    async def _table_exists(self) -> bool:
        table_exists = await self._conn.fetchrow(f"""
            select exists (
               select from pg_tables
               where schemaname = 'public'
               and   tablename  = '{self.table_name}'
           );
        """)
        return table_exists.get('exists') if table_exists else False

    async def save_migration(self, id: str):
        if not self._table_checked and not await self._table_exists():
            await self._conn.execute(f'''
                create table "{self.table_name}"
                (
                    id varchar
                        constraint pgtool_migration_pk
                            primary key,
                    time_migrated timestamp default now() not null,
                    info jsonb default '{{}}'::jsonb,
                    "order" serial not null
                );
            ''')
            self._table_checked = True
        if not self._stmt_insert:
            self._stmt_insert = await self._conn.prepare(f'insert into "{self.table_name}"(id) values ($1)')
        await self._stmt_insert.fetchval(id)
