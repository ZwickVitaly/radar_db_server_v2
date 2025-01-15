import clickhouse_connect
from settings import CLICKHOUSE_CONFING, logger


def setup_database():
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFING)
    logger.info("Setup start")

    client.command(
        '''
        CREATE TABLE IF NOT EXISTS product_data(
            wb_id UInt32 CODEC(LZ4HC),
            date Date CODEC(LZ4HC),
            size String CODEC(LZ4HC),
            warehouse UInt16 Codec(LZ4HC),
            price UInt32 Codec(LZ4HC),
            quantity UInt32 Codec (LZ4HC),
            orders UInt32 Codec (LZ4HC)
        ) ENGINE = MergeTree()
        PRIMARY KEY (wb_id, date, size, warehouse)
        ORDER BY (wb_id, date, size, warehouse);
        '''
    )
    logger.info("Tables created successfully.")
    tables = client.query("SHOW TABLES")
    logger.info(tables.result_rows)
    request_product_cols = client.query('''SELECT name, type 
       FROM system.columns 
       WHERE database = 'default' AND table = 'product_data';''')
    logger.info(request_product_cols.result_rows)
    rows_count = client.query('''SELECT count(*) FROM product_data;''')
    logger.info(rows_count.result_rows)
    client.close()

setup_database()