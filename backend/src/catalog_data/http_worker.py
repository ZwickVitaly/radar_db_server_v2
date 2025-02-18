from asyncio import sleep as asleep, Queue
from config.settings import logger


async def get_catalog_data(http_session, catalog_id: int, catalog_shard: str, catalog_query: tuple[str]|list[str], page: int , today_date):
    result_status = 0
    count = 1
    content = ""
    while result_status != 200 or count < 5:
        try:
            async with http_session.get(
                f"https://catalog.wb.ru/catalog/{catalog_shard}/v2/catalog",
                params={
                    "appType": 64,
                    "curr": "rub",
                    "dest": -1257786,
                    "spp": 30,
                    catalog_query[0]: catalog_query[1],
                    "page": page,
                    "limit": 100
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
            for i, p in enumerate(data, 1):
                p_id = p.get("id", 0)
                log = p.get("log", dict())
                nat = log.get("position", 0)
                ad_type = log.get("tp", 'z')
                place = i + (100 * (page - 1))
                cpm = log.get("cpm", 0)
                new_data.append((p_id, today_date, catalog_id, place, ad_type, nat, cpm))
            return new_data
        except Exception as error:
            logger.error(
                f"Error on catalog: {result_status} ::: (Error: {error}) sleep: {count * 0.5} (Content: {content})"
            )
            _data = ""
            if "ClientConnectionError" in str(content) and count > 1:
                return []
            result_status = 0
            await asleep(5)
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
        catalog_info = await http_queue.get()
        if catalog_info is None:
            await http_queue.put(catalog_info)
            return
        catalog_data = []
        for i in range(1, 21):
            new_catalog_data: list[tuple] = await get_catalog_data(
                http_session=http_session,
                catalog_id=catalog_info["catalog_id"],
                catalog_shard=catalog_info["catalog_shard"],
                catalog_query=catalog_info["catalog_query"],
                page=i,
                today_date=today_date,
            )
            await asleep(0.5)
            catalog_data.extend(new_catalog_data)
            if catalog_data and not i % 10:
                cd_to_write = [item for item in catalog_data]
                await save_to_db_queue.put(cd_to_write)
                catalog_data = []