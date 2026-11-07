FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Run as a fixed non-root user. The application logs to stdout, which Docker
# captures per container.
RUN groupadd --system --gid 1001 app \
    && useradd --system --uid 1001 --gid app --create-home app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app . .

# Collect DRF/admin static assets for WhiteNoise. Gunicorn does not serve
# static files itself, and runserver is intentionally not used in containers.
# One gthread worker keeps the in-process admission counter accurate. The
# thread pool must exceed MAX_CONCURRENT_CLIENTS with headroom: a 2PC prepare
# can wait on a hot row, and a commit that releases that row must never queue
# behind blocked prepares.
# App startup validates SHARD_ID against the shard map, so give collectstatic
# a valid placeholder owner; the collected assets are shard-independent.
RUN SHARD_ID=shard-a python manage.py collectstatic --noinput \
    && chown -R app:app /app

USER app

EXPOSE 8000

CMD ["gunicorn", "banking_server.wsgi:application", \
     "--bind", "0.0.0.0:8000", \
     "--workers", "1", \
     "--threads", "48", \
     "--worker-class", "gthread", \
     "--timeout", "60", \
     "--graceful-timeout", "30", \
     "--log-level", "info"]
