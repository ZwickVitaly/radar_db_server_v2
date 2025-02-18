import requests
import asyncio

from config.settings import CATALOG_ITEM_TABLE_NAME, logger
from db.connections import get_async_connection


def get_catalog_items_by_elem(catalog_data: list[dict], parent_name=None):
    result = []
    for item in catalog_data:
        children = item.get("childs")
        name = f"/{item.get("name")}"
        if parent_name:
            name = f"{parent_name}{name}"
        if children:
            children_list = get_catalog_items_by_elem(children, name)
            result.extend(children_list)
        else:
            deny_link = item.get("isDenyLink")
            if deny_link:
                continue
            else:
                shard = item.get("shard")
                if not shard:
                    continue
                else:
                    item["name"] = name
                    result.append(item)
    return result


async def get_catalog_items():
    response = await asyncio.to_thread(
        requests.get,
        url='https://static-basket-01.wbbasket.ru/vol0/data/main-menu-ru-ru-v3.json'
    )

    if response.status_code == 200:
        response_result = response.json()
        result = get_catalog_items_by_elem(response_result)
    else:
        raise ValueError("Нет ответа от json с каталогами")
    return result


def prepare_catalog_items(catalog_items: list[dict]):
    result = [
        (
            item.get('id'),
            item.get('name'),
            item.get('url'),
            item.get('shard'),
            item.get('query')
        ) for item in catalog_items
    ]
    return result


async def insert_catalog_items(catalog_items: list[tuple]):
    async with get_async_connection() as conn:
        await conn.insert(CATALOG_ITEM_TABLE_NAME, column_names=["id", "name", "url", "shard", "query"], data=catalog_items)
        await conn.command(f"""OPTIMIZE TABLE {CATALOG_ITEM_TABLE_NAME} FINAL""")


async def update_catalog_items():
    try:
        catalog_items = await get_catalog_items()
        prepared_catalog_items = prepare_catalog_items(catalog_items)
        await insert_catalog_items(prepared_catalog_items)
    except Exception as e:
        logger.critical("CAN'T UPDATE CATALOGS")
        logger.exception(e)
        raise e


if __name__ == '__main__':
    x = asyncio.run(get_catalog_items())
    for item in x:
        print(item)