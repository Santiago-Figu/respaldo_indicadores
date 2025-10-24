from fastapi import FastAPI
import uvicorn

from app.core.settings.environments import settings
#routers
from app.infrastructure.api.v1.routers import testing, athena, sin_indicadores

env = f"/{settings.ENVIRONMENT}" if settings.ENVIRONMENT != 'prod' else ""

app = FastAPI(
    title="Indicadores Excelencia Operativa",
    description= f"API para consultas Athena",
    version=settings.VERSIONAPP,
    docs_url=f"{env}{settings.API_PREFIX}/docs",
    redoc_url=f"{env}{settings.API_PREFIX}/redoc"
)

app.include_router(testing.router,prefix=settings.API_PREFIX)
app.include_router(athena.router, prefix=settings.API_PREFIX)
app.include_router(sin_indicadores.router,prefix=settings.API_PREFIX)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
