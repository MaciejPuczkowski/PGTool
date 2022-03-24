import aiofiles
from pydantic import BaseModel, Field
from typing import List, Optional, Any
from asyncpg.connection import Connection


class Migration(BaseModel):
    id: str
    dependencies: Optional[List[str]] = Field(default_factory=list)

    async def migrate(self, driver: Any):
        pass


class FileMigration(Migration):
    path: str
    encoding: Optional[str] = 'utf-8'

    def __init__(self, **kwargs):
        if 'id' not in kwargs:
            kwargs['id'] = kwargs.get('path')
        super().__init__(**kwargs)

    async def migrate(self, driver: Connection):
        async with aiofiles.open(self.path, mode='r', encoding=self.encoding) as f:
            await driver.execute(await f.read())



