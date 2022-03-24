from typing import List, Optional, Dict, AsyncGenerator
from asyncpg import Connection
from .migration_driver import MigrationDriver, AsyncpgMigrationDriver
from .exceptions import LastMigrationNotUsed, MigrationIncorrectHead
from .migrations import Migration


class AlreadyExists(Exception):
    pass


class DependencyDoesNotMet(Exception):
    pass


class Migrator:
    def __init__(self, driver: MigrationDriver):
        self._migrations: Dict[str, Migration] = {}
        self._head = None
        self._driver: MigrationDriver = driver

    def get_migration_ids_sorted(self, head: Optional[str] = None) -> List[str]:
        """
        Returns migration ids of the head sorted in a order that they should be performed
        :param head: [optional]the migration id that we want to migrate to, if None, the head is the last migration
        provided by use_migration method
        :return: List[str]
        """
        if not self._head:
            raise
        stack = [head or self._head]
        result = []
        while len(stack) > 0:
            h = stack.pop()
            result.append(h)
            stack.extend(self._migrations[h].dependencies)
        sanitized = []
        for m in reversed(result):
            if m not in sanitized:
                sanitized.append(m)
        return sanitized

    def use_migration(self, migration: Migration, depends_on_head=False):
        """
        Add a migration to the migrations' tree.
        :param migration: Migration - a migration to add to the tree
        :raises:
            - AlreadyExists - if the migration with given id has been already added
            - DependencyDoesNotMet - if the migration depends on id that has not been provided
        """
        assert isinstance(migration, Migration), 'wrong type of migration instance provided'
        if migration.id in self._migrations:
            raise AlreadyExists(f'cannot add migration {migration.id}: already exists')

        if depends_on_head and self._head:
            migration.dependencies.append(self._head)

        for dependecy in migration.dependencies:
            if dependecy not in self._migrations:
                raise DependencyDoesNotMet(
                    f'not all dependencies of the migration {migration.id} has been met, raised on: {dependecy}'
                )
        self._migrations[migration.id] = migration
        self._head = migration.id

    async def _migrate_one(self, migration: Migration):
        await migration.migrate(await self._driver.get_connection())
        await self._driver.save_migration(migration.id)

    async def migrate(self, head: Optional[str] = None) -> AsyncGenerator[str, None]:
        """
        Migrates to head or the id provided as a head.
        :param head: [optional] head of migrations if None, then it's the last migration provided by use_migration
        :return: List[str] list id migrations' ids that has been applied by migrate
        :raises MigrationIncorrectHead: when no migration has been used before the migrate command (empty migrations)
                                        or the provided head doesn't exist
        """
        head = head or self._head
        if not head or head not in self._migrations:
            raise MigrationIncorrectHead('no migration used')
        for migration_id in await self.get_next_migration_ids(head=head):
            await self._migrate_one(self._migrations[migration_id])
            yield migration_id

    async def get_next_migration_ids(self, head: Optional[str] = None) -> List[str]:
        last_applied = await self.get_last_applied_migration()
        migration_ids_sorted = self.get_migration_ids_sorted(head=head)
        if not last_applied:
            return migration_ids_sorted
        ind = migration_ids_sorted.index(last_applied.id)
        return migration_ids_sorted[ind + 1:]

    async def get_last_applied_migration(self) -> Optional[Migration]:
        """
        returns last migrated migration. It should ensure that the migration has been applied in database
        """
        last_from_db = await self._driver.get_last_migration()
        if not last_from_db:
            return None

        last_migration = self._migrations.get(last_from_db)
        if not last_migration:
            raise LastMigrationNotUsed('last migration to the db is not used in the tree')

        return last_migration


class AsyncpgMigrator(Migrator):
    def __init__(self, conn: Connection, table_name: str = None):
        super().__init__(AsyncpgMigrationDriver(conn, table_name=table_name))


