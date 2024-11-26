import boto3
import pandas as pd
import json
from datetime import datetime
import os
from typing import List, Dict, Any

class DynamoDBExporter:
    def __init__(self, table_names: List[str]):
        """        
        Inicializa el exportador de datos de DynamoDB

        Args:
            table_names: Lista de tablas a importar en DynamoDB
        """
        self.dynamodb = boto3.resource('dynamodb', 'us-east-1')
        self.table_names = table_names
        self.s3 = boto3.client('s3', 'us-east-1')
        self.bucket_name = "spotify-data-dev"

        
        self.output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

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
            response = table.scan()
            items.extend(response['Items'])
            
            while 'LastEvaluatedKey' in response:
                response = table.scan(
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])
                
            print(f"Escaneo completo de {len(items)} registros de {table_name}")
            return items
            
        except Exception as e:
            print(f"Error scaneando {table_name}: {str(e)}")
            return []

    def normalize_data(self, items: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Normalizar registros DynamoDB a pandas DataFrame
        
        Args:
            items: Lista de registros de DynamoDB
            
        Returns:
            Pandas DataFrame normalizado
        """
        df = pd.json_normalize(items)
        return df

    def upload_to_s3(self, file_path: str, filename: str):
        """
        Sube archivo a S3
        
        Args:
            file_path: Ruta local del archivo
            filename: Nombre del archivo
        """
        try:
            s3_key = f"users/{filename}"
            self.s3.upload_file(file_path, self.bucket_name, s3_key)
            print(f"Archivo subido exitosamente a s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error subiendo archivo a S3: {str(e)}")

    def export_data(self, df: pd.DataFrame, table_name: str):
        """
        Exporta data normalizada a archivo '.csv'
        
        Args:
            df: Pandas DataFrame a exportar
            table_name: Nombre de la tabla
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{table_name}_{timestamp}"
        local_path = os.path.join(self.output_dir, filename)
        
        try:
            df.to_csv(local_path, index=False)
                
            print(f"Se exportó {table_name} a {local_path}")
            
            self.upload_to_s3(local_path, filename)

        except Exception as e:
            print(f"Error exportando {table_name}: {str(e)}")

    def run(self):
        """
        Correr el proceso completo de exportación
        """
        for table_name in self.table_names:
            print(f"\Procesando tabla: {table_name}")
            
            # Scan table
            items = self.scan_table(table_name)
            if not items:
                continue
                
            # Normalize data
            df = self.normalize_data(items)
            
            # Export data
            self.export_data(df, table_name)

if __name__ == "__main__":
    TABLE_NAMES = ["dev-fp-t_users", "dev-fp-t_user_replays"]
    
    # Initialize and run exporter
    exporter = DynamoDBExporter(table_names=TABLE_NAMES)
    exporter.run()
