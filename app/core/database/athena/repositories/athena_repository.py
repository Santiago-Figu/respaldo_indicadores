from typing import Dict, Any, List
from app.core.database.athena.athena_factory import athena_factory
from app.core.models.athena_models import QueryRequest

class AthenaRepository:
    def __init__(self):
        self.factory = athena_factory
    
    def health_check(self, database_key: str = "bustrax") -> Dict[str, Any]:
        """
        Health check para verificar conexión a una base de datos configurada desde settings
        """
        client = self.factory.get_client(database_key)
        return client.test_connection()
    
    def health_check_all(self) -> Dict[str, Any]:
        """
        Health check para todas las bases de datos configuradas desde settings
        """
        results = {}
        available_dbs = self.factory.get_available_databases()
        
        for db_key in available_dbs.keys():
            try:
                client = self.factory.get_client(db_key)
                results[db_key] = client.test_connection()
            except Exception as e:
                results[db_key] = {
                    "status": "error",
                    "message": str(e)
                }
        
        return results
    
    def execute_query(self, query_request: QueryRequest) -> Dict[str, Any]:
        """
        Ejecutar consulta en una base de datos configurada desde settings
        """
        client = self.factory.get_client(query_request.database_key)
        return client.execute_query(query_request.query)
    
    def list_available_databases(self) -> Dict[str, str]:
        """
        Lista todas las bases de datos configuradas desde settings
        """
        return self.factory.get_available_databases()
    

    def get_query_results(self, database_key: str, query_execution_id: str) -> Dict[str, Any]:
        """
        Obtiene resultados de una consulta por su ID
        """
        client = self.factory.get_client(database_key)
        return client.get_query_results(query_execution_id)

    def execute_and_wait_query(self, query_request: QueryRequest) -> Dict[str, Any]:
        """
        Ejecuta consulta SQL y espera por los resultados, por defecto tiene un timeout de 300, dado por la configuración de athena
        """
        client = self.factory.get_client(query_request.database_key)
        
        # Ejecuta la consulta
        execution_result = client.execute_query(query_request.query)
        
        if execution_result["status"] == "error":
            return execution_result
        
        # Recupera la query_execution_id y manda una consulta al servidor para obtener los resultados de la misma
        query_execution_id = execution_result["query_execution_id"]
        return client.wait_for_query_completion(
            query_execution_id, 
            timeout=query_request.timeout or 300
        )