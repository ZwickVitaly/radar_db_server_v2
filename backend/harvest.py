from datetime import datetime, timedelta
import asyncio
from aiohttp import ClientSession
from clickhouse_connect.driver import AsyncClient

from db import get_async_connection
from db_worker import save_to_db
from settings import logger


async def get_products_data(http_session, products, today_date):
    result_status = 0
    products_dict = {key: val for p in products for key, val in p.items()}
    count = 1
    content = ""
    while result_status != 200 or count < 5:
        try:
            async with http_session.get(
                "https://card.wb.ru/cards/v2/detail",
                params={
                    "appType": 1,
                    "curr": "rub",
                    "dest": -1257786,
                    "spp": 30,
                    "nm": ";".join([str(key) for key in products_dict]),
                },
            ) as resp:
                result_status = resp.status
                content = resp.content
                _data = await resp.json()
                if result_status != 200:
                    continue
            data = _data.get("data").get("products")
            if not data:
                print("no data")
                return []
            new_data = []
            for p in data:
                p_id = p.get("id", 0)
                latest_data = products_dict.pop(p_id, None)
                for size in p.get("sizes"):
                    price = size.get("price", {}).get("total")
                    orig_name = size.get("origName", "0")
                    stocks = size.get("stocks", [])
                    for wh in stocks:
                        wh_id = wh.get("wh")
                        qty = wh.get("qty")
                        if latest_data:
                            prev_data = latest_data.get(f"{orig_name}_{wh_id}", {})
                            if not price:
                                price = prev_data.get("price", 0)
                            orders = max(prev_data.get("quantity", 0) - qty, 0)
                        else:
                            orders = 0

                        new_data.append(
                            (
                                p_id,  # wb_id
                                today_date,  # date
                                orig_name,  # size
                                wh_id,  # warehouse
                                price,  # price
                                qty,  # quantity
                                orders,  # orders
                            )
                        )
            return new_data
        except Exception as error:
            print(
                f"Error on products: {result_status} ::: (Error: {error}) sleep: {count * 0.5} (Content: {content})"
            )
            _data = ""
            if "ClientConnectionError" in str(content) and count > 1:
                return []
            result_status = 0
            await asyncio.sleep(count * 0.5)
            count += 1
    return []


async def get_products_page_queue(
    http_queue: asyncio.Queue,
    save_to_db_queue: asyncio.Queue,
    http_session,
    today_date,
):
    logger.info("http_worker ok")
    while True:
        product_batch = await http_queue.get()
        if product_batch is None:
            await http_queue.put(product_batch)
            return
        new_products_data = await get_products_data(
            http_session=http_session,
            products=product_batch,
            today_date=today_date,
        )
        if new_products_data:
            await save_to_db_queue.put(new_products_data)



async def get_today_products_data(left, right):
    logger.info(f"Начался сбор по товарам: {left} - {right}")
    async with get_async_connection() as client:
        client: AsyncClient = client
        now = datetime.now()
        # if now.hour < 10:
        #     today = now.date() - timedelta(days=1)
        #     yesterday = datetime.now().date() - timedelta(days=2)
        # else:
        today = now.date()
        yesterday = datetime.now().date()
        save_queue = asyncio.Queue(2)
        http_queue = asyncio.Queue(10)
        page_size = 100000
        if right - left < page_size:
            page_size = right - left
        batch_size = 500

        db_save_task = asyncio.create_task(
            save_to_db(
                queue=save_queue,
                table="product_data",
                fields=["wb_id", "date", "size", "warehouse", "price", "quantity", "orders"],
                client=client)
        )

        async with ClientSession() as http_session:
            http_get_tasks = [
                asyncio.create_task(
                    get_products_page_queue(
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
                query = f"""
                SELECT  
                    pd.wb_id as wb_id, 
                    groupArray((pd.size, pd.warehouse, pd.quantity, pd.price)) AS yesterday_data
                FROM 
                    product_data AS pd 
                WHERE 
                    pd.wb_id BETWEEN {range_id + 1} AND {range_id + page_size}
                AND pd.date = '{str(yesterday)}'
                GROUP BY
                    pd.wb_id
                ORDER BY pd.wb_id;"""
                while True:
                    try:
                        print("trying to fetch")
                        q = await client.query(query)
                        result = q.result_rows
                        print("db fetch!")
                        break
                    except Exception as e:
                        print(f"Фетч из бд: {e}")
                        await asyncio.sleep(1)
                result_dict = {
                    wb_id: {
                        f"{day[0]}_{day[1]}": {
                            "quantity": day[2],
                            "price": day[3]
                        }
                    }
                    for wb_id, yesterday_data in result for day in yesterday_data
                }
                products = [
                    {wb_id: result_dict.get(wb_id)}
                    for wb_id in range(range_id + 1, range_id + page_size + 1)
                ]
                for i in range(0, len(products) + batch_size, batch_size):
                    batch = products[i : i + batch_size]
                    if batch:
                        await http_queue.put(batch)
            await http_queue.put(None)
            await asyncio.gather(*http_get_tasks)
            await save_queue.put(None)
            await asyncio.gather(db_save_task)
