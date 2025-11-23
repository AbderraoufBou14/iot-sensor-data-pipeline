from datetime import datetime, timezone
from fastapi import APIRouter
from app.core.config import get_settings

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    settings = get_settings()
    return {
        "status": "ok",
        "service": "iot_silver_api",
        "env": settings.ENV,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
