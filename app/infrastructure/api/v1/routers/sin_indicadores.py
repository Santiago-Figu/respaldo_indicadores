from fastapi import APIRouter, Depends, HTTPException, Response, status, Query
from fastapi.responses import JSONResponse
from app.core.models.athena_models import QueryRequest
from app.core.services.alerta_clientes_service import AlertaClientesService
from app.core.settings.environments import settings

#metricas

router = APIRouter(prefix="/no-consolidados", tags=["Sin Indicadores", "Reportes"])

def get_alerta_clientes_service() -> AlertaClientesService:
    return AlertaClientesService()

# @router.get("/alerta-clientes/reporte")
# async def reporte_alerta_clientes():
#     """
#     Genera un reporte xls con la información del cliente
#     """

#     response = {"message": "Reporte generado correctamente"}
#     return JSONResponse(content = response, status_code = 200)


@router.get("/alerta-clientes/reporte")
async def generar_reporte_alerta_clientes(
    alerta_clientes_service: AlertaClientesService = Depends(get_alerta_clientes_service)
):
    """
    Genera el reporte específico de alerta_clientes usando Polars
    """
    result = alerta_clientes_service.generar_reporte_alerta_clientes("bustrax")
    
    if result["status"] == "error":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result["message"]
        )
    
    return Response(
        content=result["data"],
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={result['report_name']}",
            "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        }
    )

