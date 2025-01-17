from config.settings import logger, MAIN_TABLE_NAME
from db.connections import get_sync_connection


def setup_database():
    logger.info("Setup start")
    with get_sync_connection() as client:
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {MAIN_TABLE_NAME}(
                wb_id UInt32 CODEC(LZ4HC),
                date Date CODEC(LZ4HC),
                size String CODEC(LZ4HC),
                warehouse UInt32 Codec(LZ4HC),
                price UInt32 Codec(LZ4HC),
                quantity UInt32 Codec (LZ4HC),
                orders UInt32 Codec (LZ4HC)
            ) ENGINE = MergeTree()
            PRIMARY KEY (wb_id, date, size, warehouse)
            ORDER BY (wb_id, date, size, warehouse);
            """
        )
        logger.info("Tables created successfully.")
        tables = client.query("SHOW TABLES")
        logger.info(tables.result_rows)
        request_product_cols = client.query(
            f"""SELECT name, type 
            FROM system.columns 
            WHERE database = 'default' AND table = '{MAIN_TABLE_NAME}';"""
        )
        logger.info(request_product_cols.result_rows)
        rows_count = client.query(f"""SELECT count(*) FROM {MAIN_TABLE_NAME};""")
        logger.info(rows_count.result_rows)
        client.close()

if __name__ == "__main__":
    setup_database()
