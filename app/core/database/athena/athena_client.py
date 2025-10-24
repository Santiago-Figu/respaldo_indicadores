from typing import Dict, Any
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pathlib import Path
from app.core.models.athena_models import AthenaConnectionConfig
from app.core.logger.config import LoggerConfig

# Nota: Path(__file__).stem == __name__.split('.')[-1]
logger = LoggerConfig(file_name=Path(__file__).stem,debug=True,root_file=__name__).get_logger()

class AthenaClient:
    def __init__(self, config: AthenaConnectionConfig):
        self.config = config
        self._client = None
        
    @property
    def client(self):
        if self._client is None:
            logger.info("Iniciando cliente Athena...")
            try:
                session = boto3.Session(
                    aws_access_key_id=self.config.aws_access_key_id,
                    aws_secret_access_key=self.config.aws_secret_access_key,
                    region_name=self.config.region
                )
                self._client = session.client('athena')
            except NoCredentialsError:
                logger.error("Credenciales AWS no encontradas")
                raise
        return self._client
    
    def test_connection(self) -> Dict[str, Any]:
        """
        Test connection to specific Athena database
        """
        try:
            response = self.client.list_databases(
                CatalogName='AwsDataCatalog',
                MaxResults=20
            )
            
            databases = [db['Name'] for db in response.get('DatabaseList', [])]
            database_exists = self.config.database in databases
            
            return {
                "status": "success",
                "message": f"Conexión a Athena establecida correctamente",
                "database": self.config.database,
                "database_exists": database_exists,
                "available_databases": databases[:10]
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"Error conectando a Athena: {error_code} - {error_message}")
            
            return {
                "status": "error",
                "message": f"Error de conexión: {error_message}",
                "error_code": error_code,
                "database": self.config.database
            }
        except Exception as e:
            logger.error(f"Error inesperado conectando a Athena: {str(e)}")
            return {
                "status": "error",
                "message": f"Error inesperado: {str(e)}",
                "database": self.config.database
            }
    
    def execute_query(self, query: str) -> Dict[str, Any]:
        """
        Ejecutar consulta en la base de datos específica
        """
        try:
            # Iniciar ejecución de query
            response = self.client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': self.config.database
                },
                ResultConfiguration={
                    'OutputLocation': self.config.s3_output_location
                }
            )
            
            query_execution_id = response['QueryExecutionId']
            
            return {
                "status": "success",
                "query_execution_id": query_execution_id,
                "database": self.config.database
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"Error ejecutando query: {error_code} - {error_message}")
            
            return {
                "status": "error",
                "message": f"Error ejecutando query: {error_message}",
                "error_code": error_code
            }
        
    def get_query_results(self, query_execution_id: str) -> Dict[str, Any]:
        """
        Obtiene los resultados de una consulta ejecutada
        """
        try:
            response = self.client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            # Procesar resultados
            rows = []
            for row in response['ResultSet']['Rows']:
                row_data = [data.get('VarCharValue', '') for data in row['Data']]
                rows.append(row_data)
            
            # La primera fila son los nombres de las columnas
            columns = rows[0] if rows else []
            data_rows = rows[1:] if len(rows) > 1 else []
            
            return {
                "status": "success",
                "query_execution_id": query_execution_id,
                "columns": columns,
                "data": data_rows,
                "row_count": len(data_rows)
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"Error obteniendo resultados: {error_code} - {error_message}")
            
            return {
                "status": "error",
                "message": f"Error obteniendo resultados: {error_message}",
                "error_code": error_code
            }

    def wait_for_query_completion(self, query_execution_id: str, timeout: int = 300) -> Dict[str, Any]:
        """
        Espera a que la consulta termine y devuelve los resultados
        """
        import time
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Verificar estado de la consulta
                response = self.client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                state = response['QueryExecution']['Status']['State']
                
                if state in ['SUCCEEDED']:
                    # Consulta completada, obtener resultados
                    return self.get_query_results(query_execution_id)
                elif state in ['FAILED', 'CANCELLED']:
                    error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    return {
                        "status": "error",
                        "message": f"Query failed: {error_message}",
                        "query_state": state
                    }
                # Si está en RUNNING o QUEUED, continuar esperando
                logger.warning(f"Consulta en cola de espera, state: {state}")
                time.sleep(2)
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']
                return {
                    "status": "error",
                    "message": f"Error checking query status: {error_message}",
                    "error_code": error_code
                }
        
        return {
            "status": "error",
            "message": f"Query timeout after {timeout} seconds"
        }