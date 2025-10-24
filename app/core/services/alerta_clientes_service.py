import csv
import io
from pathlib import Path
import polars as pl
from io import BytesIO
from typing import Dict, Any
import datetime as dt
from app.core.services.athena_service import AthenaService
from app.core.models.athena_models import QueryRequest
from app.core.logger.config import LoggerConfig

#schemas del cliente

from app.domain.schemas.viajes_facturacion import schema_vf,rename_vf

# Nota: Path(__file__).stem == __name__.split('.')[-1]
logger = LoggerConfig(file_name=Path(__file__).stem,debug=True,root_file=__name__).get_logger()

class AlertaClientesService:
    def __init__(self, athena_service: AthenaService = None):
        self.athena_service = athena_service or AthenaService()
    
    def generar_reporte_alerta_clientes(self, database_key: str = "bustrax") -> Dict[str, Any]:
        """
        Genera el reporte específico de alerta_clientes con query fija usando Polars
        """
        try:
            logger.info("Realizando cálculo de fechas")
            # Cálculo de fechas (preservado para comparativa del usuario)
            N = 8  # número de semanas completas a considerar
            
            fecha_ini = dt.date.today() - dt.timedelta(days=dt.date.today().weekday() + (N * 7))
            fecha_fin = fecha_ini + dt.timedelta(days=(N * 7) - 1)
            
            semanas_lst = pl.date_range(start=fecha_ini, end=fecha_ini + pl.duration(weeks=N-1), interval='1w', eager=True)
            archivo_salida = 'alerta_clientes_' + semanas_lst[-1].strftime('%y%m%d') + '.xlsx'
            
            # query unica para reemplaza las 56 consultas individuales (8 semanas × 7 días) generadas por el ciclo
            logger.info("Generando query")
            query = f"""
            SELECT *
            FROM viajes_facturacion
            WHERE start_date >= '{fecha_ini.strftime('%Y-%m-%d')}'
            AND start_date <= '{fecha_fin.strftime('%Y-%m-%d')}'
            """

            # Ejecutar consulta
            query_request = QueryRequest(
                database_key=database_key,
                query=query,
                timeout=300
            )
            logger.info("Ejecutando query")
            result = self.athena_service.execute_and_wait_query(query_request)
            
            if result["status"] == "error":
                return result
            
            # Realizar operaciones específicas con Polars
            processed_data = self._procesar_datos_alerta_clientes(result, semanas_lst)
            
            # Generar Excel
            logger.info("Generando documento xlsx")
            excel_buffer = self._generar_excel(processed_data)
            
            return {
                "status": "success",
                "report_name": archivo_salida,
                "row_count": processed_data.height,
                "file_size": len(excel_buffer.getvalue()),
                "data": excel_buffer.getvalue()
            }
            
        except Exception as e:
            logger.error(f"Error generando reporte alerta_clientes: {str(e)}")
            return {
                "status": "error",
                "message": f"Error generando reporte: {str(e)}"
            }
    
    # def _procesar_datos_alerta_clientes(self, query_result: Dict[str, Any]) -> pl.DataFrame:
    #     """
    #     Realiza operaciones específicas para el reporte alerta_clientes usando Polars
    #     """
    #     columns = query_result.get("columns", [])
    #     data = query_result.get("data", [])
        
    #     # Crear DataFrame de Polars
    #     # df = pl.DataFrame(data, schema=columns, orient="row")
    #     df = pl.DataFrame(data, schema=schema_vf, orient="row")
        
    #     # Aquí puedes realizar todas las operaciones que necesites con Polars
        

    #     # Ejemplo: Agregar columna de identificador
    #     df = df.with_columns([
    #         pl.Series("id_reporte", range(1, df.height + 1))
    #     ])
        
    #     return df

    def _procesar_datos_alerta_clientes(self, query_result: Dict[str, Any], semanas_lst) -> pl.DataFrame:
        """
        Realiza operaciones específicas para el reporte alerta_clientes usando Polars
        """
        logger.info("Procesando Datos con Polars")
        columns = query_result.get("columns", [])
        data = query_result.get("data", [])
        
        # Crear DataFrame usando el mismo método que funcionaba antes
        logger.info("Creando Data Frame")
        
        # Convertir los datos a CSV en memoria (simulando lo que hacía antes)
        
        
        # Crear un buffer CSV en memoria
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        
        # Escribir headers
        writer.writerow(columns)
        # Escribir datos
        writer.writerows(data)
        
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()
        
        # Leer usando read_csv con ignore_errors=True (método probado por el usuario)
        # Si no se usa este metodo y se intenta realizar un casteo de los datos desde un dataframe normal de polars,
        # causa muchos errores de casteo de datos dado el schema_vf que usa el usuario y a los datos vacios que contiene la tabla
        # como propuesta de mejora, se tendrían que mapear todas las columnas y realizar un tratamiento de los datos según lo que requiera el usuario dado a que
        # en este caso, existen columnas que contienen datos númericos mezclados con datos strings que ignore_errors=True se encarga de manejar y por ello no le daba 
        # errores al crear el dataframe
        # Nota: reportar esto al líder del proyecto para comparar datos en data lake.
        df = pl.read_csv(
            csv_content.encode('utf-8'),
            schema=schema_vf,
            ignore_errors=True,
            null_values=["", "NULL", "null"]
        )
        
        logger.info("Primera transformación del dataframe: vl_sem")
        vl_sem = (
            df
            .rename(rename_vf)
            .filter(
                (pl.col('status') != 9) & 
                (pl.col('tipo_de_viaje') != 'VA') & 
                ~(pl.col('cliente').str.starts_with('GRUPO VIAJES ESPECIALES - '))
            )
            .with_columns(
                fecha_ini = pl.col('fecha_ini').str.to_date(format='%Y-%m-%d'),
            )
            .sort(by=['udn', 'cliente', 'fecha_ini'], descending=[False, False, False])
            .group_by_dynamic('fecha_ini', group_by=['udn', 'cliente'], every='1w', start_by='monday', closed='left', label='left')
            .agg(viajes=pl.col('cliente').count())
        )
        
        logger.info("Segunda transformación del dataframe: udn_clientes")
        udn_clientes = (
            vl_sem
            .select(['udn', 'cliente'])
            .unique()
            .sort(by=['udn', 'cliente'], descending=[False, False])
            .join(pl.DataFrame({'fecha_ini': semanas_lst}), how='cross')
        )
        
        logger.info("Tercera transformación del dataframe: clientes_op (resultado final)")
        clientes_op = (
            udn_clientes
            .join(vl_sem, on=['udn', 'cliente', 'fecha_ini'], how='left')
            .with_columns(
                viajes=pl.col('viajes').fill_null(0)
            )
            .sort(by=['udn', 'cliente', 'fecha_ini'], descending=[False, False, False])
            .with_columns(
                viajes_prev=pl.col('viajes').shift(1).over(['udn', 'cliente'])
            )
            .with_columns(
                viajes_N_a_0=((pl.col('viajes_prev').is_not_null()) & (pl.col('viajes') == 0) & (pl.col('viajes_prev') > 0)).cast(pl.Int8),
                viajes_0_a_N=((pl.col('viajes_prev').is_not_null()) & (pl.col('viajes') > 0) & (pl.col('viajes_prev') == 0)).cast(pl.Int8),
            )
            .drop(['viajes_prev'])
        )
        
        logger.info("Data frame clientes_op creado con éxito")
        return clientes_op
    
    def _generar_excel(self, df: pl.DataFrame) -> BytesIO:
        """Genera el archivo Excel a partir del DataFrame de Polars"""
        output = BytesIO()
        
        # Polars tiene write_excel integrado
        df.write_excel(
            output,
            worksheet="reporte",
            table_name="reporte",
            autofilter=False,
            freeze_panes = (1,0),
            autofit=True,
            table_style={'style':'Table Style Medium 4', 'banded_rows': 1},  # Estilo opcional
        )
        
        output.seek(0)
        return output