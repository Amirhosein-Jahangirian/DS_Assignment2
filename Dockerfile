FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Run as a fixed non-root user. The project files are owned by it so the
# file logger can still write transaction_logs.log in this phase.
RUN groupadd --system --gid 1001 app \
    && useradd --system --uid 1001 --gid app --create-home app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=app:app . .

# WORKDIR created /app as root before the copy; make the directory itself
# writable so the non-root user can create transaction_logs.log at runtime.
RUN chown app:app /app

USER app

EXPOSE 8000

# Django's development server. Its built-in static handler serves the DRF
# browsable API and Django admin assets (available while DEBUG=True, which is
# the current setting). --noreload keeps one process so the in-process
# admission counter and the file log handler behave as before.
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000", "--noreload"]
