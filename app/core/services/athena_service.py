from typing import Dict, Any
from app.core.database.athena.repositories.athena_repository import AthenaRepository

from app.core.models.athena_models import QueryRequest

class AthenaService:
    def __init__(self, athena_repository: AthenaRepository = None):
        self.athena_repository = athena_repository or AthenaRepository()
    
    def check_connection(self, database_key: str = "bustrax") -> Dict[str, Any]:
        """
        Verifica la conexión a una base de datos deseada de Athena
        """
        return self.athena_repository.health_check(database_key)
    
    def check_all_connections(self) -> Dict[str, Any]:
        """
        Verifica la conexión a todas las bases de datos configuradas en settings
        """
        return self.athena_repository.health_check_all()
    
    def execute_query(self, query_request: QueryRequest) -> Dict[str, Any]:
        """
        Ejecuta una consulta en la base de datos deseada de Athena
        """
        return self.athena_repository.execute_query(query_request)
    
    def get_available_databases(self) -> Dict[str, str]:
        """
        Obtiene la lista de bases de datos disponibles en settings
        """
        return self.athena_repository.list_available_databases()
    
    def get_query_results(self, database_key: str, query_execution_id: str) -> Dict[str, Any]:
        """
        Obtiene resultados de una consulta por su ID de ejecución para verificación
        """
        return self.athena_repository.get_query_results(database_key, query_execution_id)

    def execute_and_wait_query(self, query_request: QueryRequest) -> Dict[str, Any]:
        """
        Ejecuta consulta SQL en texto y espera por los resultados (síncrono)
        """
        return self.athena_repository.execute_and_wait_query(query_request)