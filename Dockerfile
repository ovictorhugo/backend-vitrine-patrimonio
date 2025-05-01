FROM python:3.12-slim

ENV POETRY_VIRTUALENVS_CREATE=false

WORKDIR /app

COPY . .

RUN pip install poetry && \
    poetry config installer.max-workers 10 && \
    poetry install --no-interaction --no-ansi 

EXPOSE 8080

CMD ["gunicorn", "-b", "0.0.0.0:8080", "vitrine:create_app()", "--reload", "--log-level", "info", "--access-logfile", "access.log", "--error-logfile", "error.log", "--workers", "4", "--timeout", "6000"]
