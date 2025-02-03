import datetime

from clickhouse_connect.driver import AsyncClient
from asyncio import sleep

from config.settings import MAIN_TABLE_NAME


async def create_temp_table_wb_id_existing_today(
    client: AsyncClient, today_date, temp_table_name, table_name
):
    if temp_table_name == "product_data":
        raise ValueError
    drop_table_query = f"""
            DROP TABLE IF EXISTS {temp_table_name};
            """
    await client.command(drop_table_query)
    temp_table_query = f"""
        CREATE TABLE IF NOT EXISTS {temp_table_name}(
            wb_id UInt32 CODEC(LZ4HC)
        ) ENGINE = MergeTree()
        PRIMARY KEY (wb_id)
        ORDER BY (wb_id);
    """
    await client.command(temp_table_query)
    temp_table_insert_query = f"""
        INSERT INTO {temp_table_name}(wb_id) 
        SELECT wb_id FROM {table_name} WHERE date = '{str(today_date)}' 
        GROUP BY wb_id;
    """
    await client.command(temp_table_insert_query)


async def drop_temp_table(
    client: AsyncClient,
    temp_table_name,
):
    if temp_table_name == MAIN_TABLE_NAME:
        raise ValueError
    drop_table_query = f"""
        DROP TABLE IF EXISTS {temp_table_name};
    """
    await client.command(drop_table_query)

async def get_temp_table_existing_ids(
    temp_table_name: str,
    left: int,
    right: int,
    client: AsyncClient
):
    query = f"""
        SELECT  
            wb_id
        FROM 
            {temp_table_name}
        WHERE 
            wb_id BETWEEN {left} AND {right};"""
    while True:
        try:
            print("trying to fetch temp table")
            q = await client.query(query)
            existing_wb_id = {row[0] for row in q.result_rows}
            if existing_wb_id:
                print("GOT TODAY PRODUCTS")
            else:
                print("No products data for today")
            break
        except Exception as e:
            print(f"Фетч из бд temp: {e}")
            await sleep(1)
    return existing_wb_id


async def get_existing_ids(
    table_name: str,
    left: int,
    right: int,
    day: datetime.date,
    client: AsyncClient
):
    query = f"""
        SELECT  
            wb_id
        FROM 
            {table_name}
        WHERE 
            wb_id BETWEEN {left} AND {right}
        AND
            date = '{str(day)}'
        GROUP BY wb_id;"""
    while True:
        try:
            print("trying to fetch temp table")
            q = await client.query(query)
            existing_wb_id = {row[0] for row in q.result_rows}
            if existing_wb_id:
                print("GOT TODAY PRODUCTS")
            else:
                print("No products data for today")
            break
        except Exception as e:
            print(f"Фетч из бд temp: {e}")
            await sleep(1)
    return existing_wb_id