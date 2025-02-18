from config.settings import logger, PRODUCT_DATA_TABLE_NAME, CATALOG_DATA_TABLE_NAME, CATALOG_ITEM_TABLE_NAME
from db.connections import get_sync_connection


def setup_database():
    logger.info("Setup start")
    with get_sync_connection() as client:
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {PRODUCT_DATA_TABLE_NAME}(
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
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {CATALOG_ITEM_TABLE_NAME}(
                id UInt32 CODEC(LZ4HC),
                updated DateTime DEFAULT now() CODEC(LZ4HC),
                name String CODEC(LZ4HC),
                url String CODEC(LZ4HC),
                shard String CODEC(LZ4HC),
                query String CODEC(LZ4HC),
            ) ENGINE = ReplacingMergeTree(updated)
            PRIMARY KEY (id)
            ORDER BY (id);
            """
        )
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {CATALOG_DATA_TABLE_NAME}(
                wb_id UInt32 CODEC(LZ4HC),
                date Date CODEC(LZ4HC),
                catalog_id UInt32 CODEC(LZ4HC),
                place UInt32 Codec(LZ4HC),
                advert FixedString(1) Codec(LZ4HC),
                natural_place UInt32 DEFAULT 0 Codec (LZ4HC),
                cpm UInt32 DEFAULT 0 CODEC(LZ4HC)
            ) ENGINE = MergeTree()
            PRIMARY KEY (wb_id, date, catalog_id)
            ORDER BY (wb_id, date, catalog_id);
            """
        )
        logger.info("Tables created successfully.")
        tables = client.query("SHOW TABLES")
        logger.info(tables.result_rows)
        request_product_cols = client.query(
            f"""SELECT name, type 
            FROM system.columns 
            WHERE database = 'default' AND table = '{PRODUCT_DATA_TABLE_NAME}';"""
        )
        logger.info(request_product_cols.result_rows)
        rows_count = client.query(f"""SELECT count(*) FROM {PRODUCT_DATA_TABLE_NAME};""")
        logger.info(rows_count.result_rows)
        client.close()

if __name__ == "__main__":
    setup_database()
