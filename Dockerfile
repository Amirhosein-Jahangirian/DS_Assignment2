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

# WORKDIR created /app as root before the copy; hand the directory to app.
RUN chown app:app /app

USER app

EXPOSE 8000

# Django's development server. Its built-in static handler serves the DRF
# browsable API and Django admin assets (available while DEBUG=True, which is
# the current setting). --noreload keeps one process so the in-process
# admission counter stays accurate.
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000", "--noreload"]
