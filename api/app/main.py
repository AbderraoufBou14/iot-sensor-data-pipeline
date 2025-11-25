from fastapi import FastAPI

from app.routers import health
from dev.api.app.routers import silver_routes

def create_app() -> FastAPI:
    app = FastAPI(
        title="IoT Silver API",
        version="0.1.0",
        description="API détaillée pour explorer les données dans la couche silver.",
    )

    @app.get("/", tags=["root"])
    def root():
        return {"message": "IoT Silver API - see /docs"}

    # Mount routers (sans préfix v1)
    app.include_router(health.router)
    app.include_router(silver_routes.router, prefix="/silver")

    return app

app = create_app()
