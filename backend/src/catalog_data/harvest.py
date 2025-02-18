from datetime import datetime
import asyncio
from aiohttp import ClientSession

from config.settings import logger, CATALOG_DATA_TABLE_NAME, CATALOG_ITEM_TABLE_NAME
from src.catalog_data.get_recent_catalog import update_catalog_items
from src.catalog_data.http_worker import http_worker
from db.connections import get_async_connection
from src.common.db_worker import save_to_db_worker



async def get_today_catalog_data():
    logger.info(f"Начался сбор по каталогам")
    try:
        await update_catalog_items()
    except Exception as e:
        logger.critical("Can't update catalogs")
        logger.exception(e)
        return
    async with get_async_connection() as client:
        catalogs_query = await client.query(
            f"""SELECT id, shard, query FROM {CATALOG_ITEM_TABLE_NAME} FINAL"""
        )
        catalogs = catalogs_query.result_rows
        today = datetime.now().date()
        save_queue = asyncio.Queue(2)
        http_queue = asyncio.Queue(10)
        main_table_name = CATALOG_DATA_TABLE_NAME
        db_save_task = asyncio.create_task(
            save_to_db_worker(
                queue=save_queue,
                table=main_table_name,
                fields=[
                    "wb_id",
                    "date",
                    "catalog_id",
                    "place",
                    "advert",
                    "natural_place",
                    "cpm",
                ],
                client=client,
            )
        )

        async with ClientSession() as http_session:
            http_get_tasks = [
                asyncio.create_task(
                    http_worker(
                        http_queue=http_queue,
                        save_to_db_queue=save_queue,
                        http_session=http_session,
                        today_date=today,
                    )
                )
                for _ in range(4)
            ]
            for catalog_info in catalogs:
                cat_id = catalog_info[0]
                shard = catalog_info[1]
                query = catalog_info[2]
                if "&" in query:
                    split_query = catalog_info[2].split("&")
                    query = []
                    for part in split_query:
                        left, right = part.split("=")
                        query.append(left)
                        query.append(right)
                else:
                    query = query.split("=")
                await http_queue.put(
                    {
                        "catalog_id": cat_id,
                        "catalog_shard": shard,
                        "catalog_query": query,
                    }
                )
            await http_queue.put(None)
            await asyncio.gather(*http_get_tasks)
            await save_queue.put(None)
            await asyncio.gather(db_save_task)
    return