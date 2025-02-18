import asyncio
from datetime import datetime

from src.catalog_data.harvest import get_today_catalog_data
from src.wb_products_history.harvest import get_today_products_data
from celery_main import celery_app
from config.settings import logger


@celery_app.task(name="products_data_get")
def products_data_get():
    start_time = datetime.now()
    logger.info(f"Вход в harvest product")
    asyncio.run(get_today_products_data(0, 500000000))
    end_time = datetime.now()
    delta = (end_time - start_time).seconds
    logger.info(
        f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Выполнено за: {delta // 60 // 60} часов, {delta // 60 % 60} минут"
    )


@celery_app.task(name="catalog_data_get")
def catalog_data_get():
    start_time = datetime.now()
    logger.info(f"Вход в harvest catalog")
    asyncio.run(get_today_catalog_data())
    end_time = datetime.now()
    delta = (end_time - start_time).seconds
    logger.info(
        f"Старт парса: {start_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Завершение парса: {end_time.strftime('%H:%M %d.%m.%Y')}\n"
        f"Выполнено за: {delta // 60 // 60} часов, {delta // 60 % 60} минут"
    )


