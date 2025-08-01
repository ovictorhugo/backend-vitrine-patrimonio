import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from vitrine.routers import assets, auth, catalog, inventory, users
from vitrine.routers.organizational_structure import (
    agencies,
    legal_guardians,
    location,
    materials,
    sectors,
    units,
)

storage_path = os.path.join(os.path.dirname(__file__), 'storage')
os.makedirs(storage_path, exist_ok=True)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

app.include_router(users.router)
app.include_router(auth.router)
app.include_router(assets.router)
app.include_router(catalog.router)
app.include_router(inventory.router)

app.include_router(agencies.router)
app.include_router(units.router)
app.include_router(sectors.router)
app.include_router(location.router)
app.include_router(legal_guardians.router)
app.include_router(materials.router)


@app.get('/')
def root():
    return {'mensagem': 'API em funcionamento!'}
