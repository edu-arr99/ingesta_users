import boto3
import pandas as pd
import json
from datetime import datetime
import os
from typing import List, Dict, Any
import logging
import sys
from logging.handlers import RotatingFileHandler
import time

class DynamoDBExporter:
    def __init__(self, table_names: List[str], container_name: str):
        """        
        Inicializa el exportador de datos de DynamoDB

        Args:
            table_names: Lista de tablas a importar en DynamoDB
            container_name: Nombre del contenedor para los logs
        """
        self.dynamodb = boto3.resource('dynamodb', 'us-east-1')
        self.table_names = table_names
        self.s3 = boto3.client('s3', 'us-east-1')
        self.bucket_name = "earr99-spotify-data-prod"
        self.container_name = container_name

        
        self.output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

        self.setup_logging()


    def setup_logging(self):
        """
        Configura el sistema de logging con el formato requerido
        """
        # Crear el directorio de logs si no existe
        log_dir = "/home/ubuntu/spotify-ingestion"
        os.makedirs(log_dir, exist_ok=True)
        
        # Configurar el logger
        self.logger = logging.getLogger(self.container_name)
        self.logger.setLevel(logging.INFO)
        
        # Formato del log: fecha_hora,tipo_log,nombre_contenedor,mensaje
        log_format = '%(asctime)s,%(levelname)s,%(name)s,%(message)s'
        formatter = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S.%f')
        
        # Handler para archivo
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, f"spotify-etl.log"),
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)
        
        # Handler para consola
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)


    def scan_table(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Scan tabla DynamoDB 
        
        Args:
            table_name: Tabla a scanear
            
        Returns:
            Lista de registros de la tabla
        """
        table = self.dynamodb.Table(table_name)
        items = []
        
        try:
            self.logger.info(f"Iniciando escaneo de tabla {table_name}")
            start_time = time.time()

            response = table.scan()
            items.extend(response['Items'])
            
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])
                
            duration = time.time() - start_time
            self.logger.info(f"Escaneo completo. Total registros: {len(items)}. Duración: {duration:.2f} segundos")
            return items
            
        except Exception as e:
            self.logger.error(f"Error escaneando {table_name}: {str(e)}")
            return []


    def normalize_data(self, items: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalizar registros DynamoDB a pandas DataFrame
        
        Args:
            items: Lista de registros de DynamoDB
            
        Returns:
            Pandas DataFrame normalizado
        """
        self.logger.info(f"Iniciando normalización de {len(items)} registros")
        try:
            df = pd.json_normalize(items)
            self.logger.info(f"Normalización completada. Columnas resultantes: {', '.join(df.columns)}")
            return df
        except Exception as e:
            self.logger.error(f"Error en normalización: {str(e)}")
            raise

    def upload_to_s3(self, file_path: str, filename: str, folder: str):
        """
        Sube archivo a S3
        
        Args:
            file_path: Ruta local del archivo
            filename: Nombre del archivo
            folder: Carpeta destino en S3
        """
        try:
            self.logger.info(f"Iniciando carga a S3 del archivo {filename}")
            s3_key = f"{folder}/{filename}"
            self.s3.upload_file(file_path, self.bucket_name, s3_key)
            self.logger.info(f"Archivo cargado exitosamente a s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            self.logger.error(f"Error cargando archivo a S3: {str(e)}")
            raise

    def export_data(self, df: pd.DataFrame, table_name: str, folder: str):
        """
        Exporta data normalizada a archivo CSV
        
        Args:
            df: Pandas DataFrame a exportar
            table_name: Nombre de la tabla
            folder: Carpeta destino en S3
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{table_name}_{timestamp}.csv"
        local_path = os.path.join(self.output_dir, filename)
        
        try:
            self.logger.info(f"Iniciando exportación de {len(df)} registros a CSV")
            df.to_csv(local_path, index=False)
            self.logger.info(f"Datos exportados localmente a {local_path}")
            
            self.upload_to_s3(local_path, filename, folder)

        except Exception as e:
            self.logger.error(f"Error en exportación: {str(e)}")
            raise

    def run(self):
        """
        Correr el proceso completo de exportación
        """
        self.logger.info(f"Iniciando proceso de exportación para {len(self.table_names)} tablas")
        start_time = time.time()
        
        try:
            for table_name in self.table_names:
                self.logger.info(f"=== Procesando tabla: {table_name} ===")
                
                # Scan table
                items = self.scan_table(table_name)
                if not items:
                    self.logger.warning(f"No se encontraron registros en {table_name}")
                    continue
                    
                # Normalize data
                df = self.normalize_data(items)
                
                # Export data
                folder = table_name.split('_')[2]  # Obtiene 'users', 'songs', etc.
                self.export_data(df, table_name, folder)
                
                self.logger.info(f"=== Procesamiento de {table_name} completado ===")
            
            duration = time.time() - start_time
            self.logger.info(f"Proceso de exportación completado. Duración total: {duration:.2f} segundos")
            
        except Exception as e:
            self.logger.critical(f"Error fatal en el proceso: {str(e)}")
            raise

if __name__ == "__main__":
    TABLE_NAMES = ["dev-fp-t_users", "dev-fp-t_user_replays"]
    
    # Initialize and run exporter
    exporter = DynamoDBExporter(table_names=TABLE_NAMES, container_name="ingesta_users")
    exporter.run()
