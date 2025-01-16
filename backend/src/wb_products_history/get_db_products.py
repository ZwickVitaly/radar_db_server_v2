from asyncio import sleep
from datetime import date

from clickhouse_connect.driver import AsyncClient


async def get_day_db_products(
    table_name: str,
    left: int,
    right: int,
    day: date,
    client: AsyncClient
):
    query = f"""
        SELECT  
            pd.wb_id as wb_id, 
            groupArray((pd.size, pd.warehouse, pd.quantity, pd.price)) AS yesterday_data
        FROM 
            {table_name} AS pd 
        WHERE 
            pd.wb_id BETWEEN {left} AND {right}
        AND pd.date = '{str(day)}'
        GROUP BY
            pd.wb_id
        ORDER BY pd.wb_id;"""
    while True:
        try:
            print("trying to fetch")
            q = await client.query(query)
            result = q.result_rows
            if result:
                print("db fetch!")
            else:
                print("no products in yesterday")
            break
        except Exception as e:
            print(f"Фетч из бд: {e}")
            await sleep(1)
    return result
