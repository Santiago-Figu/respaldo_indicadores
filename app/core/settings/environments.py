import json
import os
from pydantic_settings import BaseSettings
from pathlib import Path
from typing import Dict, Optional
from app.utils.utils import find_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent

class Settings(BaseSettings):
    ENVIRONMENT: str = "development"
    SERVICE: str = "service_name"
    VERSIONAPP: str = "0.0.0"

    #S3
    AWS_ACCESS_KEY_ID: str = 'tu_secret_key'
    AWS_SECRET_ACCESS_KEY: Optional[str] = 'tu_secret_key'
    AWS_REGION: Optional[str] = 'us-west-2'
    ATHENA_DEFAULT_DATABASE: Optional[str] = 's3_st1_bustrax'
    ATHENA_S3_OUTPUT_LOCATION: Optional[str] = 's3://tu-bucket-query-results/'
    
    # Para manejo de múltiples bases de datos Athena 
    ATHENA_DATABASES: Optional[str] = '{"bustrax": "s3_bustrax", "analytics": "s3_prod_analytics"}'

    #FastApi
    API_PREFIX: str = '/demo/api/v1'

    # Auth
    SECRET_KEY: str = 'tu_secret_key'
    FERNET_KEY: str = 'tu_fernet_key'
    ENCODING: Optional[str] = 'utf-8'
    
    # Configuración MongoDB
    MONGO_USER: Optional[str] = None
    MONGO_PASSWORD: Optional[str] = None
    MONGO_SERVER: str = 'localhost'
    MONGO_PORT: str = '27017'
    MONGO_DB: str = 'tu_db'

    @property
    def athena_databases(self) -> Dict[str, str]:
        """Parse ATHENA_DATABASES from JSON string to dict"""
        if self.ATHENA_DATABASES:
            return json.loads(self.ATHENA_DATABASES)
        return {"default": self.ATHENA_DEFAULT_DATABASE or "default"}
    
    # Solo para uso en local, colocar en la raiz el archivo .env deseado
    class Config:
        env_file = BASE_DIR / '.env' if (BASE_DIR / '.env').exists() else None

settings = Settings()