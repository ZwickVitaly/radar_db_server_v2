import gc
from asyncio import Queue as AsyncQueue
from clickhouse_connect.driver import AsyncClient
from config.settings import logger


async def save_to_db_worker(queue: AsyncQueue, table: str, fields: list[str], client: AsyncClient):
    while True:
        items = []
        item = []
        while len(items) < 10000:
            item = await queue.get()
            if item is None:
                break
            items.extend(item)
        if items:
            try:
                await client.insert(table, items, column_names=fields)
                logger.info(f"Запись в БД +")
                gc.collect()
            except Exception as e:
                logger.critical(f"{e}, {items}")
        if item is None:
            await queue.put(item)
            return
