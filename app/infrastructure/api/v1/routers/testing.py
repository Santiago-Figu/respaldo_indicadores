import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from app.core.settings.environments import settings

router = APIRouter(prefix="/test", tags=["Testing"])

@router.get("/health")
async def health_check():
    """
    Health check endpoint para monitoreo
    """

    response = {
        "status": "healthy",
        "timestamp": str(datetime.datetime.now()),
        "service": settings.SERVICE
    }

    status = 200

    return JSONResponse(content=response, status_code=status)
