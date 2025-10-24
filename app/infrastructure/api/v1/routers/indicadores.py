from fastapi import APIRouter, Depends, HTTPException, status, Query
from app.core.models.athena_models import QueryRequest
from app.core.settings.environments import settings


# metricas
router = APIRouter(prefix="/consolidados", tags=["Indicadores"])