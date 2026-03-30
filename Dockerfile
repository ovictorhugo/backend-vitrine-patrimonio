FROM python:3.13-slim AS builder

ENV POETRY_VIRTUALENVS_CREATE=false \
    PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on

WORKDIR /app

RUN pip install --no-cache-dir poetry

COPY poetry.lock pyproject.toml ./

RUN poetry config installer.max-workers 10 \
    && poetry install --no-interaction --no-ansi --without dev --no-root \
    && poetry add polars-lts-cpu

FROM python:3.13-slim

ENV POETRY_VIRTUALENVS_CREATE=false \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    libcairo2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgdk-pixbuf-2.0-0 \
    libgobject-2.0-0 \
    libffi-dev \
    shared-mime-info \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY . .

COPY entrypoint.sh .
RUN chmod +x ./entrypoint.sh

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "--host", "0.0.0.0", "--port", "8000", "--reload", "--workers", "4", "vitrine.app:app"]
