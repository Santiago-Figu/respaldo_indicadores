from fastapi import APIRouter, Depends, HTTPException, status, Query
from app.core.services.athena_service import AthenaService
from app.core.models.athena_models import QueryRequest
from app.core.settings.environments import settings

router = APIRouter(prefix="/athena", tags=["AWS Athena"])

def get_athena_service() -> AthenaService:
    return AthenaService()

@router.get("/health")
async def athena_health_check(
    database: str = Query("bustrax", description="Clave de la base de datos"),
    athena_service: AthenaService = Depends(get_athena_service)
):
    """
    Verifica la conexión con una base de datos específica de AWS Athena
    """
    result = athena_service.check_connection(database)
    
    if result["status"] == "error":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=result["message"]
        )
    
    return {
        "service": "aws-athena",
        "database_key": database,
        "database_name": result.get("database", "unknown"),
        "status": "connected" if result["status"] == "success" else "error",
        "details": result
    }

@router.get("/health/all")
async def athena_health_check_all(
    athena_service: AthenaService = Depends(get_athena_service)
):
    """
    Verifica la conexión con TODAS las bases de datos configuradas
    """
    results = athena_service.check_all_connections()
    return {
        "service": "aws-athena",
        "status": "completed",
        "results": results
    }

if settings.ENVIRONMENT == 'development' or settings.ENVIRONMENT == "devel":

    @router.get("/databases")
    async def list_available_databases(
        athena_service: AthenaService = Depends(get_athena_service)
    ):
        """
        Lista todas las bases de datos disponibles en variables de entorno
        """
        databases = athena_service.get_available_databases()
        return {
            "available_databases": databases
        }

    @router.post("/query")
    async def execute_query(
        query_request: QueryRequest,
        athena_service: AthenaService = Depends(get_athena_service)
    ):
        """
        Ejecuta una consulta en una base de datos específica
        """
        result = athena_service.execute_query(query_request)
        
        if result["status"] == "error":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result["message"]
            )
        
        return result

    @router.get("/query/{query_execution_id}")
    async def get_query_results(
        query_execution_id: str,
        database: str = Query("bustrax", description="Clave de la base de datos"),
        athena_service: AthenaService = Depends(get_athena_service)
    ):
        """
        Obtiene los resultados de una consulta por su ID de ejecución
        """
        result = athena_service.get_query_results(database, query_execution_id)
        
        if result["status"] == "error":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result["message"]
            )
        
        return result

    @router.post("/query/sync")
    async def execute_query_sync(
        query_request: QueryRequest,
        athena_service: AthenaService = Depends(get_athena_service)
    ):
        """
        Ejecuta una consulta y espera por los resultados
        """
        result = athena_service.execute_and_wait_query(query_request)
        
        if result["status"] == "error":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result["message"]
            )
        
        return result