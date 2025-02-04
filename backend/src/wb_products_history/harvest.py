from datetime import datetime, timedelta
import asyncio
from aiohttp import ClientSession
from clickhouse_connect.driver import AsyncClient

from config.settings import logger, MAIN_TABLE_NAME
from src.wb_products_history.get_db_products import get_day_db_products
from src.wb_products_history.http_worker import http_worker
from db.connections import get_async_connection
from src.wb_products_history.db_worker import save_to_db_worker
from src.wb_products_history.temp_table import (
    get_existing_ids
)


async def get_today_products_data(left, right):
    logger.info(f"Начался сбор по товарам: {left} - {right}")
    async with get_async_connection() as client:
        client: AsyncClient = client
        now = datetime.now()
        if now.hour >= 10:
            today = now.date()
        else:
            today = now.date() - timedelta(days=1)
        yesterday = today - timedelta(days=1)
        save_queue = asyncio.Queue(2)
        http_queue = asyncio.Queue(10)
        main_table_name = MAIN_TABLE_NAME
        # temp_table_name = "temp_table_wb_id_today"
        # await create_temp_table_wb_id_existing_today(
        #     client=client,
        #     today_date=today,
        #     temp_table_name=temp_table_name,
        #     table_name=main_table_name
        # )
        page_size = 100000
        batch_size = 500
        db_save_task = asyncio.create_task(
            save_to_db_worker(
                queue=save_queue,
                table=main_table_name,
                fields=[
                    "wb_id",
                    "date",
                    "size",
                    "warehouse",
                    "price",
                    "quantity",
                    "orders",
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
            for range_id in range(left, right, page_size):
                print(f"batch: {range_id}")
                yesterday_data = await get_day_db_products(
                    table_name=main_table_name,
                    left=range_id + 1,
                    right=range_id + page_size,
                    day=yesterday,
                    client=client
                )
                existing_wb_id = await get_existing_ids(
                    table_name=main_table_name,
                    day=today,
                    left=range_id + 1,
                    right=range_id + page_size,
                    client=client
                )
                result_dict = {
                    wb_id: {f"{size[0].strip()}_{size[1]}": {"quantity": size[2], "price": size[3]} for size in yesterday_item}
                    for wb_id, yesterday_item in yesterday_data
                }
                products = [
                    {wb_id: result_dict.get(wb_id)}
                    for wb_id in range(range_id + 1, range_id + page_size + 1)
                    if wb_id not in existing_wb_id
                ]
                for i in range(0, len(products) + batch_size, batch_size):
                    batch = products[i : i + batch_size]
                    if batch:
                        await http_queue.put(batch)
            await http_queue.put(None)
            await asyncio.gather(*http_get_tasks)
            await save_queue.put(None)
            await asyncio.gather(db_save_task)
            # await drop_temp_table(client=client, temp_table_name=temp_table_name)
    return