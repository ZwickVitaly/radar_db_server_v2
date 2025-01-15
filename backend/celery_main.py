import os

from celery import Celery
from celery.schedules import crontab
from settings import REDIS_HOST

celery_app = Celery(
    "harvester",
    include=[
        "actions",
    ],
)

celery_app.conf.broker_url = os.environ.get(
    "CELERY_BROKER_URL", f"redis://{REDIS_HOST}:6379"
)
celery_app.conf.result_backend = os.environ.get(
    "CELERY_RESULT_BACKEND", f"redis://{REDIS_HOST}:6379"
)
celery_app.conf.broker_connection_retry_on_startup = True

celery_app.conf.beat_schedule = {
    "products_data_get": {
        "task": "products_data_get",
        "schedule": crontab(hour="11", minute="7",),
    },
}