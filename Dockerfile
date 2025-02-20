FROM python:3.12-alpine

ENV PYTHONUNBUFFERED 1

WORKDIR /app

RUN apk add build-base
RUN apk add python3-dev
RUN apk add musl-dev
RUN apk add linux-headers
RUN pip install poetry
COPY pyproject.toml poetry.lock ./
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi --no-root

COPY ./backend /app

ENTRYPOINT python server.py