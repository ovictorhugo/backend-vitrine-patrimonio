from fastapi import APIRouter

from . import catalog as sub_catalog
from . import (
    catalog_files,
    catalog_images,
    catalog_search,
    catalog_transfers,
    catalog_workflow,
)

router = APIRouter()

router.include_router(catalog_files.router)
router.include_router(catalog_images.router)
router.include_router(catalog_search.router)
router.include_router(catalog_transfers.router)
router.include_router(catalog_workflow.router)

router.include_router(sub_catalog.router)
