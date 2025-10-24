# FROM python:3.11-slim-bookworm
FROM python:3.13.9

EXPOSE 8010
EXPOSE 8011

# Variables de entorno
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOME=/app \
    PIP_NO_CACHE_DIR=1 \
    TZ=America/Mexico_City

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libpq-dev tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    rm -rf /var/lib/apt/lists/*

COPY ./docker/requirements.txt .
RUN pip install --upgrade pip && \
    pip install -r requirements.txt --no-cache-dir
COPY . .

# Usuario no root
RUN adduser -u 5678 --disabled-password --gecos "" appuser && \
    chown -R appuser $APP_HOME
USER appuser