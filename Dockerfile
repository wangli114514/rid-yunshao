# syntax=docker/dockerfile:1.7

FROM python:3.12-slim-bookworm

ARG APP_UID=10001
ARG APP_GID=10001

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --requirement requirements.txt

RUN groupadd --gid "${APP_GID}" rid \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" \
        --home-dir /app --no-create-home --shell /usr/sbin/nologin rid \
    && mkdir -p /app/data \
    && chown -R "${APP_UID}:${APP_GID}" /app

COPY --chown=${APP_UID}:${APP_GID} server.py dashboard.html demo.html airspace_catalog.json ./
COPY --chown=${APP_UID}:${APP_GID} airspace-data ./airspace-data

USER ${APP_UID}:${APP_GID}

EXPOSE 18081 18082

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:18081/healthz', timeout=3).close()"]

STOPSIGNAL SIGTERM

ENTRYPOINT ["python", "server.py"]
CMD ["--cloud", "--bind", "0.0.0.0", "--http", "18081", "--ws", "18082", "--db", "/app/data/rid_history.db", "--flight-gap", "15", "--point-distance", "2", "--point-interval", "5", "--retention-days", "30"]
