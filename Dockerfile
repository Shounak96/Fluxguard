FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY fluxguard ./fluxguard
COPY migrations ./migrations
COPY alembic.ini ./
RUN pip install --upgrade pip && pip install .

RUN useradd --create-home --uid 10001 fluxguard && chown -R fluxguard:fluxguard /app
USER fluxguard

CMD ["uvicorn", "fluxguard.apps.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
