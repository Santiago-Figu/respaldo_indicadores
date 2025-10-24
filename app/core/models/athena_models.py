from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class AthenaDatabase(BaseModel):
    name: str = Field(..., description="Nombre identificador de la base de datos")
    database_name: str = Field(..., description="Nombre real en Athena")
    description: Optional[str] = None
    
class AthenaConnectionConfig(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: Optional[str] = None
    region: str = "us-west-2"
    database: str
    s3_output_location: Optional[str] = None

class QueryRequest(BaseModel):
    database_key: str = Field(..., description="Clave de la base de datos (bustrax, analytics, etc.)")
    query: str = Field(..., description="Consulta SQL a ejecutar")
    timeout: Optional[int] = Field(600, description="Timeout en segundos")

class QueryResultRequest(BaseModel):
    database_key: str = Field(..., description="Clave de la base de datos")
    query_execution_id: str = Field(..., description="ID de ejecución de la consulta")