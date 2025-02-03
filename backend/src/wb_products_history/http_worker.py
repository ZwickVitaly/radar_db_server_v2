from asyncio import sleep as asleep, Queue
from config.settings import logger


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
                return []
            new_data = []
            for p in data:
                p_id = p.get("id", 0)
                latest_data = products_dict.pop(p_id, None)
                for size in p.get("sizes"):
                    price = size.get("price", {}).get("total")
                    orig_name = size.get("origName", "0").strip()
                    stocks = size.get("stocks", [])
                    for wh in stocks:
                        wh_id = wh.get("wh")
                        qty = wh.get("qty")
                        if latest_data:
                            prev_data = latest_data.pop(f"{orig_name}_{wh_id}", {})
                            if not price:
                                price = prev_data.get("price", 0)
                            latest_qty = prev_data.get("quantity", 0)
                            orders = max(latest_qty - qty, 0)
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
                if latest_data:
                    for key, val in latest_data.items():
                        size, wh = key.split("_")
                        quantity = val.get("quantity", 0)
                        if not quantity or quantity > 100:
                            continue
                        price = val.get("price", 0)
                        new_data.append((p_id, today_date, size, int(wh), price, 0, quantity))
            return new_data
        except Exception as error:
            logger.error(
                f"Error on products: {result_status} ::: (Error: {error}) sleep: {count * 0.5} (Content: {content})"
            )
            _data = ""
            if "ClientConnectionError" in str(content) and count > 1:
                return []
            result_status = 0
            await asleep(count * 0.5)
            count += 1
    return []


async def http_worker(
    http_queue: Queue,
    save_to_db_queue: Queue,
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