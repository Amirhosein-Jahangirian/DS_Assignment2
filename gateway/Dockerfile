FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    CONFIG_DIR=/app/config

RUN groupadd --system --gid 1001 app \
    && useradd --system --uid 1001 --gid app --create-home app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app . .

USER app

EXPOSE 8000

# Access logs are disabled: the gateway already emits structured request
# logs, and the Docker healthcheck hits /health every few seconds.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
