#!/bin/sh

alembic upgrade head

uvicorn --host 0.0.0.0 --port 8000 vitrine.app:app --workers 4