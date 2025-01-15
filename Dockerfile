FROM python:3.12

ENV PYTHONUNBUFFERED 1

WORKDIR /app
RUN pip install poetry
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --no-root

COPY ./backend /app

ENTRYPOINT python setup.py && uvicorn app:app --host 0.0.0.0 --port 9013