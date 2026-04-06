"""FastAPI application factory."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from catalog_api_studio.api.routes import router


def create_app() -> FastAPI:
    app = FastAPI(
        title="Catalog API Studio",
        description="REST API for digitized product catalogs",
        version="0.1.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)

    return app
