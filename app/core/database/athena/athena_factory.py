from typing import Dict, Optional
from app.core.database.athena.athena_client import AthenaClient
from app.core.models.athena_models import AthenaConnectionConfig
from app.core.settings.environments import settings

class AthenaClientFactory:
    def __init__(self):
        self._clients: Dict[str, AthenaClient] = {}
        self.available_databases = settings.athena_databases
    
    def get_client(self, database_key: str) -> AthenaClient:
        """
        Obtiene un cliente de tipo Athena para la base de datos solicitada
        """
        if database_key not in self._clients:
            if database_key not in self.available_databases:
                raise ValueError(f"Base de datos '{database_key}' no configurada")
            
            database_name = self.available_databases[database_key]
            
            config = AthenaConnectionConfig(
                aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
                region=settings.AWS_REGION,
                database=database_name,
                s3_output_location=settings.ATHENA_S3_OUTPUT_LOCATION
            )
            
            self._clients[database_key] = AthenaClient(config)
        
        return self._clients[database_key]
    
    def get_available_databases(self) -> Dict[str, str]:
        """Retorna las bases de datos disponibles"""
        return self.available_databases

# Instancia global de AthenaClientFactory
athena_factory = AthenaClientFactory()