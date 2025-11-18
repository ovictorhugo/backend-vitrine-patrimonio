import os
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from vitrine.core.database import get_session
from vitrine.core.settings import Settings
from vitrine.events import check_and_update_stale_workflows
from vitrine.routers import (
    assets,
    auth,
    catalog,
    collection,
    collection_items,
    favorite,
    feedback,
    inventory,
    notifications,
    rbac,
    system,
    users,
    users_visuals,
)
from vitrine.routers.organizational_structure import (
    agencies,
    legal_guardians,
    location,
    materials,
    sectors,
    units,
)
from vitrine.routers.statistics import catalog_statistics

SETTINGS = Settings()

BASE_DIR = os.path.dirname(__file__)
STORAGE_DIR = os.path.join(BASE_DIR, 'storage')
UPLOADS_DIR = os.path.join(STORAGE_DIR, 'uploads')
TEMP_DIR = os.path.join(STORAGE_DIR, 'temp')

for path in [STORAGE_DIR, UPLOADS_DIR, TEMP_DIR]:
    os.makedirs(path, exist_ok=True)

print(f'🧭 Servindo uploads a partir de: {UPLOADS_DIR}')


scheduler = AsyncIOScheduler(timezone='America/Sao_Paulo')


@scheduler.scheduled_job(CronTrigger(hour=0, minute=0, second=0))
async def migration_to_alienation():
    async for session in get_session():
        count = await check_and_update_stale_workflows(session)
        print(f'🔹 {count} workflows atualizados para ALIENACAO.')


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.environ.get('ENVIRONMENT') != 'PYTEST':
        scheduler.start()
        print('🔹 APScheduler iniciado')
    yield
    if os.environ.get('ENVIRONMENT') != 'PYTEST':
        scheduler.shutdown(wait=False)
        print('🔹 Encerrando APScheduler...')


app = FastAPI(
    root_path=SETTINGS.ROOT_PATH,
    debug=False,
    lifespan=lifespan,
)


app.mount('/uploads', StaticFiles(directory=STORAGE_DIR), name='uploads')


app.add_middleware(
    CORSMiddleware,
    allow_origins=SETTINGS.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

app.include_router(users.router)
app.include_router(users_visuals.router)
app.include_router(auth.router)
app.include_router(rbac.router)
app.include_router(catalog_statistics.router)
app.include_router(assets.router)
app.include_router(catalog.router)
app.include_router(inventory.router)
app.include_router(favorite.router)
app.include_router(agencies.router)
app.include_router(units.router)
app.include_router(sectors.router)
app.include_router(location.router)
app.include_router(legal_guardians.router)
app.include_router(materials.router)
app.include_router(collection.router)
app.include_router(collection_items.router)
app.include_router(notifications.router)
app.include_router(feedback.router)
app.include_router(system.router)


@app.get('/')
def root():
    return {'mensagem': 'API em funcionamento!'}
