FROM python:3.13-slim
ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app/
COPY . .

RUN pip install poetry

RUN poetry config installer.max-workers 10
RUN poetry install --no-interaction --no-ansi --without dev
RUN poetry add polars-lts-cpu

EXPOSE 8000
CMD ["poetry", "run", "uvicorn", "--host", "0.0.0.0", "--port", "8000", "--reload", "vitrine.app:app"]