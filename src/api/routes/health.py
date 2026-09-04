import time
from fastapi import APIRouter
from src.api.schemas import HealthResponse

health_router = APIRouter()
START_TIME = time.time()

@health_router.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(
        status="ok",
        version="1.0.0",
        uptime_seconds=time.time() - START_TIME
    )

@health_router.get("/ready")
async def readiness_probe():
    return {"status": "ready"}
