from contextlib import asynccontextmanager, contextmanager
from clickhouse_connect import get_async_client, get_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver import Client

from config.settings import CLICKHOUSE_CONFIG


@asynccontextmanager
async def get_async_connection() -> AsyncClient:
    session = AsyncSession(CLICKHOUSE_CONFIG)
    async with session as client:
        yield client


class AsyncSession:

    def __init__(self, clickhouse_config):
        self.config = clickhouse_config

    async def __aenter__(self) -> AsyncClient:
        self.client = await get_async_client(**self.config)
        return self.client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.client.close()


@contextmanager
def get_sync_connection():
    session = SyncSession(CLICKHOUSE_CONFIG)
    with session as client:
        yield client


class SyncSession:
    def __init__(self, clickhouse_config):
        self.config = clickhouse_config

    def __enter__(self) -> Client:
        self.client = get_client(**self.config)
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
