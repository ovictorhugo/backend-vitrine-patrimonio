import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from vitrine.routers import (
    assets,
    auth,
    catalog,
    collection,
    collection_items,
    favorite,
    inventory,
    notifications,
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
from vitrine.settings import Settings

BASE_DIR = os.path.dirname(__file__)
STORAGE_DIR = os.path.join(BASE_DIR, 'storage')
os.makedirs(STORAGE_DIR, exist_ok=True)

UPLOADS_DIR = os.path.join(STORAGE_DIR, 'uploads')
print(UPLOADS_DIR)
os.makedirs(UPLOADS_DIR, exist_ok=True)

TEMP_DIR = os.path.join(STORAGE_DIR, 'temp')
os.makedirs(TEMP_DIR, exist_ok=True)

app = FastAPI(root_path=Settings().ROOT_PATH, debug=True)

app.mount('/uploads', StaticFiles(directory=UPLOADS_DIR), name='uploads')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

app.include_router(users.router)
app.include_router(users_visuals.router)
app.include_router(auth.router)

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

app.include_router(catalog_statistics.router)

app.include_router(notifications.router)


@app.get('/')
def root():
    return {'mensagem': 'API em funcionamento!'}
